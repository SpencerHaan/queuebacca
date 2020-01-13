/*
 * Copyright 2020 The Queuebacca Authors
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

package io.axonif.queuebacca.subscribing;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.ExceptionResolver;
import io.axonif.queuebacca.IncomingEnvelope;
import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.MessageContext;
import io.axonif.queuebacca.MessageResponse;
import io.axonif.queuebacca.RetryDelayGenerator;
import io.axonif.queuebacca.WorkPermitHolder;
import io.axonif.queuebacca.events.TimingEventListener;
import io.axonif.queuebacca.events.TimingEventSupport;
import io.axonif.queuebacca.exceptions.QueuebaccaException;
import io.axonif.queuebacca.worker.WorkPool;

/**
 * Subscribes to {@link MessageBin MessageBins} for the purposes of consuming {@link Message ViaMessages}. Maintains
 * a list of active subscriptions for the purposes of cancelling them.
 */
public final class Subscriber {

    private final List<ActiveSubscription<?>> activeSubscriptions = new ArrayList<>();
    private final TimingEventSupport timingEventSupport = new TimingEventSupport();

    private final Client client;
	private final ExceptionResolver exceptionResolver;
    private final WorkPool workPool = WorkPool.start("queuebacca");

	private final int finalFallbackReadCount;
    private final FallbackMessageConsumerFactory finalFallbackMessageConsumerFactory;

	/**
	 * Creates a new instance of a {@link Subscriber} for a specific {@link Client}.
	 *
	 * @param client the client for the message broker
	 * @param exceptionResolver determines resolution for exceptions thrown by messages
	 */
	private Subscriber(
			Client client,
			ExceptionResolver exceptionResolver,
			int finalFallbackReadCount,
			FallbackMessageConsumerFactory finalFallbackMessageConsumerFactory
	) {
		this.client = client;
		this.exceptionResolver = exceptionResolver;
		this.finalFallbackReadCount = finalFallbackReadCount;
		this.finalFallbackMessageConsumerFactory = finalFallbackMessageConsumerFactory;
	}

	public static Builder builder(Client client) {
		return new Builder(client);
	}

	/**
	 * Subscribes to a {@link MessageBin} using the provided {@link SubscriptionConfiguration}. This will produce
	 * an active thread that will check for messages.
	 *
	 * @param configuration subscription configuration
	 */
	public <M extends Message> Subscription subscribe(SubscriptionConfiguration<M> configuration) {
        requireNonNull(configuration);

		MessageConsumerSelector<M> consumerSelector = configuration.getMessageConsumers();
        if (finalFallbackMessageConsumerFactory != null) {
        	consumerSelector = MessageConsumerSelector.builder(consumerSelector)
					.addNextConsumer(finalFallbackReadCount, finalFallbackMessageConsumerFactory.create())
					.build();
		}

        String messageBinName = configuration.getMessageBin().getName();
        ActiveSubscription<M> subscription = new ActiveSubscription<>(
				client,
				configuration.getMessageBin(),
				workPool.acquireLicense(messageBinName + "-subscription", 1),
				workPool.acquireLicense(messageBinName + "-processing", configuration.getMessageCapacity()),
				configuration.getRetryDelayGenerator(),
				consumerSelector,
				LoggerFactory.getLogger(messageBinName)
		);
        activeSubscriptions.add(subscription);
        subscription.start();
        return subscription;
    }

	/**
	 * Cancels all currently active subscriptions.
	 */
	public void cancelAll() {
        activeSubscriptions.forEach(ActiveSubscription::cancel);
        workPool.shutdown();
    }

	public void addTimingEventListener(TimingEventListener timingEventListener) {
		timingEventSupport.addListener(timingEventListener);
	}

	public void removeTimingEventListener(TimingEventListener timingEventListener) {
		timingEventSupport.removeListener(timingEventListener);
	}

	private class ActiveSubscription<M extends Message> implements Subscription {

		private final Logger logger;

		private final Client client;
		private final MessageBin messageBin;
		private final WorkPool.License subscriptionLicense;
		private final WorkPool.License processingLicense;
		private final RetryDelayGenerator retryDelayGenerator;
		private final MessageConsumerSelector<M> consumerSelector;

		private boolean running;

		ActiveSubscription(
				Client client,
				MessageBin messageBin,
				WorkPool.License subscriptionLicense,
				WorkPool.License processingLicense,
				RetryDelayGenerator retryDelayGenerator,
				MessageConsumerSelector<M> consumerSelector,
				Logger logger
		) {
			this.client = client;
			this.messageBin = messageBin;
			this.subscriptionLicense = subscriptionLicense;
			this.processingLicense = processingLicense;
			this.retryDelayGenerator = retryDelayGenerator;
			this.consumerSelector = consumerSelector;
			this.logger = logger;
		}

		@Override
		public void start() {
			if (running) {
				return; // TODO Should we throw here instead?
			}

			running = true;
			workPool.submitTask(subscriptionLicense, () -> {
				while (true) {
					try {
						check();
					} catch (InterruptedException e) {
						if (!running) {
							break;
						}
					} catch (Exception e) {
						logger.error("Exception occurred while checking a subscription", e);
					} catch (Error e) {
						logger.error("Error occurred while checking a subscription", e);
						throw e;
					}
				}
			});
		}

		@Override
		public void cancel() {
			if (!running) {
				return;
			}

			logger.info("Cancelling subscription: '{}'", messageBin.getName());
			running = false;
			workPool.cancelTasks(subscriptionLicense);
			workPool.cancelTasks(processingLicense);
		}

		private void check() throws InterruptedException {
			workPool.submitTasks(processingLicense, );
			Queue<WorkPermitHolder.Permit> permits = new LinkedList<>(workPermitHolder.acquireAvailable());
			int availableCapacity = permits.size();

			try {
				Collection<IncomingEnvelope<M>> envelopes = client.retrieveMessages(messageBin, availableCapacity);
				Collection<Runnable> workOrders = envelopes.stream()
						.map(this::newWorkOrder)
						.collect(Collectors.toList());
				if (workOrders.size() > availableCapacity) {
					throw new QueuebaccaException("The number of work orders, {0}, cannot exceed the available capacity, {1}", workOrders.size(), availableCapacity);
				}

				workOrders.forEach(workOrder -> {
					WorkPermitHolder.Permit permit = permits.remove();
					Future<?> messageProcessingHandle = workPool.submit(() -> {
						try {
							workOrder.run();
						} finally {
							permit.release();
						}
					});
					messageProcessingHandles.add(messageProcessingHandle);
				});
			} finally {
				permits.iterator().forEachRemaining(WorkPermitHolder.Permit::release);
			}
		}

		private Runnable newWorkOrder(IncomingEnvelope<M> envelope) {
			return () -> {
				MDC.put("queuebaccaMessageId", envelope.getMessageId());
				MDC.put("queuebaccaMessageReadCount", String.valueOf(envelope.getReadCount()));

				int currentReadCountThreshold = consumerSelector.currentReadCountThreshold(envelope.getReadCount());
				int currentConsumersReadCount = envelope.getReadCount() - currentReadCountThreshold;
				MessageContext messageContext = new MessageContext(envelope.getMessageId(), currentConsumersReadCount, envelope.getFirstReceived(), envelope.getRawMessage());
				try {
					MessageResponse messageResponse;
					try {
						long start = System.currentTimeMillis();
						try {
							messageResponse = consumerSelector.select(envelope.getReadCount())
									.consume(envelope.getMessage(), messageContext);
						} finally {
							long duration = System.currentTimeMillis() - start;
							timingEventSupport.fireEvent(messageBin, envelope.getMessage().getClass(), envelope.getMessageId(), duration);
						}
					} catch (Exception e) {
						messageResponse = exceptionResolver.resolve(e, messageContext);
					}
					handleResponse(messageResponse, envelope, currentConsumersReadCount);
				} catch (Error e) {
					logger.error("Error occurred while processing message: '{}' with body '{}'", new Object[]{ envelope.getMessageId(), envelope.getRawMessage(), e });
					throw e;
				} finally {
                    MDC.clear();
				}
			};
		}

		private void handleResponse(MessageResponse messageResponse, IncomingEnvelope<M> envelope, int readCount) {
			switch (messageResponse) {
				case CONSUMED:
					logger.info("Consumed '{}'; disposing", envelope.getMessageId());
					client.disposeMessage(messageBin, envelope);
					break;
				case RETRY:
					int retryDelay = retryDelayGenerator.nextRetryDelay(readCount);
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

	public static class Builder {

		private final Client client;

		private ExceptionResolver exceptionResolver = ExceptionResolver.builder().build();

		private int finalFallbackReadCount;
		private FallbackMessageConsumerFactory finalFallbackMessageConsumerFactory;

		private Builder(Client client) {
			this.client = client;
		}

		public Builder withExceptionResolver(ExceptionResolver exceptionResolver) {
			this.exceptionResolver = requireNonNull(exceptionResolver);
			return this;
		}

		public Builder withFinalFallbackMessageConsumer(int readCount, FallbackMessageConsumerFactory fallbackMessageConsumerFactory) {
			this.finalFallbackReadCount = readCount;
			this.finalFallbackMessageConsumerFactory = requireNonNull(fallbackMessageConsumerFactory);
			return this;
		}

		public Subscriber build() {
			return new Subscriber(client, exceptionResolver, finalFallbackReadCount, finalFallbackMessageConsumerFactory);
		}
	}
}
