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
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.axonif.queuebacca.events.TimingEventListener;
import io.axonif.queuebacca.events.TimingEventSupport;

/**
 * Subscribes to {@link MessageBin MessageBins} for the purposes of consuming messages. Maintains a list of active subscriptions for the purposes of
 * cancelling them.
 */
public final class Subscriber {

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
	public <Message> void subscribe(SubscriptionConfiguration<Message> configuration) {
        requireNonNull(configuration);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(configuration.getMessageBin().getName() + "-processor-%d")
                .build();
        WorkExecutor workExecutor = workExecutorFactory.newWorkExecutor(configuration.getMessageCapacity(), threadFactory);

        ActiveSubscription<Message> subscription = ActiveSubscription.start(configuration, client, workExecutor, exceptionResolver, timingEventSupport);
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

    interface SubscriptionHarness<Message> {

		void handle(IncomingEnvelope envelope, BiConsumer<IncomingEnvelope, MessageConsumer<Message>> envelopeConsumer);
	}

	static class DefaultSubscriptionHarness<Message> implements SubscriptionHarness<Message> {

		private final MessageConsumer<Message> messageConsumer;

		DefaultSubscriptionHarness(MessageConsumer<Message> messageConsumer) {
			this.messageConsumer = messageConsumer;
		}

		@Override
		public void handle(IncomingEnvelope envelope, BiConsumer<IncomingEnvelope, MessageConsumer<Message>> envelopeConsumer) {
			envelopeConsumer.accept(envelope, messageConsumer);
		}
	}

	static class FallbackSubscriptionHarness<Message> implements SubscriptionHarness<Message> {

		private final NavigableMap<Integer, MessageConsumer<? extends Message>> consumers;

		FallbackSubscriptionHarness(NavigableMap<Integer, MessageConsumer<? extends Message>> consumers) {
			this.consumers = consumers;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handle(IncomingEnvelope envelope, BiConsumer<IncomingEnvelope, MessageConsumer<Message>> envelopeConsumer) {
			int expectedTotalRead = consumers.subMap(0, envelope.getReadCount()).keySet().stream()
					.mapToInt(v -> v)
					.sum();
			MessageConsumer<Message> consumer = (MessageConsumer<Message>) consumers.floorEntry(envelope.getReadCount()).getValue();

			IncomingEnvelope incomingEnvelope = new IncomingEnvelope(
					envelope.getMessageId(),
					envelope.getReceipt(),
					envelope.getReadCount() - expectedTotalRead,
					envelope.getFirstReceived(),
					envelope.getMessageBody()
			);
			envelopeConsumer.accept(incomingEnvelope, consumer);
		}
	}
}
