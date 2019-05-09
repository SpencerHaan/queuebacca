/*
 * Copyright 2019 The Queuebacca Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axonif.queuebacca;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.axonif.queuebacca.events.TimingEventListener;
import io.axonif.queuebacca.events.TimingEventSupport;

/**
 * Subscribes to {@link MessageBin MessageBins} for the purposes of consuming {@link Message ViaMessages}. Maintains
 * a list of active subscriptions for the purposes of cancelling them.
 */
public final class Subscriber {

	private static final int SUBSCRIPTION_INITIAL_DELAY = 1000; // milliseconds
	private static final int SUBSCRIPTION_PERIODIC_DELAY = 1; // milliseconds

    private final List<ActiveSubscription<?>> activeSubscriptions = new ArrayList<>();
    private final TimingEventSupport timingEventSupport = new TimingEventSupport();

    private final Client client;
    private final WorkExecutorFactory workExecutorFactory;
    private final ExceptionResolver exceptionResolver;

	/**
	 * Creates a new instance of a {@link Subscriber} for a specific {@link Client}.
	 *
	 * @param client the client for the message broker
	 * @param workExecutorFactory factory for creating {@link WorkExecutor WorkExecutors}
	 * @param exceptionResolver determines resolution for exceptions thrown by messages
	 */
	public Subscriber(Client client, WorkExecutorFactory workExecutorFactory, ExceptionResolver exceptionResolver) {
        this.client = requireNonNull(client);
		this.workExecutorFactory = requireNonNull(workExecutorFactory);
		this.exceptionResolver = requireNonNull(exceptionResolver);
	}

	/**
	 * Subscribes to a {@link MessageBin} using the provided {@link SubscriptionConfiguration}. This will produce
	 * an active thread that will check for messages.
	 *
	 * @param configuration subscription configuration
	 */
	public void subscribe(SubscriptionConfiguration<?> configuration) {
        requireNonNull(configuration);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(configuration.getMessageBin().getName() + "-processor-%d")
                .build();
        WorkExecutor workExecutor = workExecutorFactory.newWorkExecutor(configuration.getMessageCapacity(), threadFactory);

        ActiveSubscription<?> subscription = ActiveSubscription.start(configuration, client, workExecutor, exceptionResolver, timingEventSupport);
        activeSubscriptions.add(subscription);
    }

	/**
	 * Cancels all currently active subscriptions.
	 */
	public void cancelAll() {
        activeSubscriptions.forEach(ActiveSubscription::cancel);
    }

	public void addTimingEventListener(TimingEventListener timingEventListener) {
		timingEventSupport.addListener(timingEventListener);
	}

	public void removeTimingEventListener(TimingEventListener timingEventListener) {
		timingEventSupport.removeListener(timingEventListener);
	}

	/**
	 * An active subscription that periodically checks for new messages, distributing the consumption using a {@link ThreadPoolWorkExecutor}.
	 *
	 * @param <M> the message type
	 */
	private static class ActiveSubscription<M extends Message> {

		private final Logger logger;
		private final ScheduledExecutorService subscriptionScheduler;

		private final Client client;
		private final MessageBin messageBin;
		private final WorkExecutor workExecutor;
		private final RetryDelayGenerator retryDelayGenerator;
		private final MessageConsumer<M> consumer;
		private final ExceptionResolver exceptionResolver;
		private final TimingEventSupport timingEventSupport;

		ActiveSubscription(
				ScheduledExecutorService subscriptionScheduler, Client client,
				MessageBin messageBin,
				WorkExecutor workExecutor,
				RetryDelayGenerator retryDelayGenerator,
				MessageConsumer<M> consumer,
				ExceptionResolver exceptionResolver,
				TimingEventSupport timingEventSupport,
				Logger logger
		) {
			this.subscriptionScheduler = subscriptionScheduler;
			this.client = client;
			this.messageBin = messageBin;
			this.workExecutor = workExecutor;
			this.retryDelayGenerator = retryDelayGenerator;
			this.consumer = consumer;
			this.exceptionResolver = exceptionResolver;
			this.timingEventSupport = timingEventSupport;
			this.logger = logger;
		}

		static <M extends Message> ActiveSubscription<M> start(
				SubscriptionConfiguration<M> configuration,
				Client client,
				WorkExecutor processingPool,
				ExceptionResolver exceptionResolver,
				TimingEventSupport timingEventSupport
		) {
			ScheduledExecutorService subscriptionScheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
					.setNameFormat(configuration.getMessageBin().getName() + "-subscription-%d")
					.build());

			Logger logger = LoggerFactory.getLogger(configuration.getMessageBin().getName());
			ActiveSubscription<M> subscription = new ActiveSubscription<>(
					subscriptionScheduler,
					client,
					configuration.getMessageBin(),
					processingPool,
					configuration.getRetryDelayGenerator(),
					configuration.getMessageConsumer(),
					exceptionResolver,
					timingEventSupport,
					logger
			);

			subscriptionScheduler.scheduleAtFixedRate(() -> {
				try {
					subscription.check();
				} catch (InterruptedException e) {
					// Do nothing
				} catch (Exception e) {
					logger.error("Exception occurred while checking a subscription", e);
				} catch (Error e) {
					logger.error("Error occurred while checking a subscription", e);
					throw e;
				}
			}, SUBSCRIPTION_INITIAL_DELAY, SUBSCRIPTION_PERIODIC_DELAY, TimeUnit.MILLISECONDS);
			return subscription;
		}

		void cancel() {
			logger.info("Cancelling subscription to '{}'", messageBin.getName());
			subscriptionScheduler.shutdownNow();
			workExecutor.shutdownNow();
		}

		private void check() throws InterruptedException {
			workExecutor.submitWorkOrders(capacity -> {
				Collection<IncomingEnvelope<M>> envelopes = client.retrieveMessages(messageBin, capacity);
				return envelopes.stream()
						.map(this::newWorkOrder)
						.collect(Collectors.toList());
			});
		}

		private ThreadPoolWorkExecutor.WorkOrder newWorkOrder(IncomingEnvelope<M> envelope) {
			return () -> {
				MessageContext messageContext = new MessageContext(envelope.getMessageId(), envelope.getReadCount(), envelope.getFirstReceived(), envelope.getRawMessage());
				MDC.put("queuebaccaMessageId", messageContext.getMessageId());
				MDC.put("queuebaccaMessageReadCount", String.valueOf(messageContext.getReadCount()));
				try {
					MessageResponse messageResponse;
					try {
						long start = System.currentTimeMillis();
						try {
							messageResponse = consumer.consume(envelope.getMessage(), messageContext);
						} finally {
							long duration = System.currentTimeMillis() - start;
							timingEventSupport.fireEvent(messageBin, envelope.getMessage().getClass(), envelope.getMessageId(), duration);
						}
					} catch (Exception e) {
						messageResponse = exceptionResolver.resolve(e, messageContext);
					}
					handleResponse(messageResponse, envelope);
				} catch (Error e) {
					logger.error("Error occurred while processing message: '{}' with body '{}'", new Object[]{ envelope.getMessageId(), envelope.getRawMessage(), e });
					throw e;
				} finally {
                    MDC.clear();
				}
			};
		}

		private void handleResponse(MessageResponse messageResponse, IncomingEnvelope<M> envelope) {
			switch (messageResponse) {
				case CONSUMED:
					logger.info("Consumed '{}'; disposing", envelope.getMessageId());
					client.disposeMessage(messageBin, envelope);
					break;
				case RETRY:
					int retryDelay = retryDelayGenerator.nextRetryDelay(envelope.getReadCount());
					logger.warn("Retrying Message '{}' with body {} after {} seconds", new Object[]{ envelope.getMessageId(), envelope.getRawMessage(), retryDelay });
					client.returnMessage(messageBin, envelope, retryDelay);
					break;
				case TERMINATE:
					logger.warn("Terminated '{}' with body {}; disposing", envelope.getMessageId(), envelope.getRawMessage());
					client.disposeMessage(messageBin, envelope);
					break;
				default:
					logger.error("Unknown exception resolution, {}, for '{}'  with body {}; disposing", new Object[]{messageResponse, envelope.getMessageId(), envelope.getRawMessage() });
					client.disposeMessage(messageBin, envelope);
					break;
			}
		}
	}
}
