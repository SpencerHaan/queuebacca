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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.ExceptionResolver;
import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.ThreadPoolWorkExecutor;
import io.axonif.queuebacca.WorkExecutor;
import io.axonif.queuebacca.WorkExecutorFactory;
import io.axonif.queuebacca.events.TimingEventListener;
import io.axonif.queuebacca.events.TimingEventSupport;

/**
 * Subscribes to {@link MessageBin MessageBins} for the purposes of consuming {@link Message ViaMessages}. Maintains
 * a list of active subscriptions for the purposes of cancelling them.
 */
public final class Subscriber {

    private final List<ActiveSubscription<?>> activeSubscriptions = new ArrayList<>();
    private final TimingEventSupport timingEventSupport = new TimingEventSupport();

    private final Client client;
    private final WorkExecutorFactory workExecutorFactory;
    private final ExceptionResolver exceptionResolver;

	private final int finalFallbackReadCount;
    private final FallbackMessageConsumerFactory finalFallbackMessageConsumerFactory;

	/**
	 * Creates a new instance of a {@link Subscriber} for a specific {@link Client}.
	 *
	 * @param client the client for the message broker
	 * @param workExecutorFactory factory for creating {@link WorkExecutor WorkExecutors}
	 * @param exceptionResolver determines resolution for exceptions thrown by messages
	 */
	private Subscriber(
			Client client,
			WorkExecutorFactory workExecutorFactory,
			ExceptionResolver exceptionResolver,
			int finalFallbackReadCount,
			FallbackMessageConsumerFactory finalFallbackMessageConsumerFactory
	) {
		this.client = client;
		this.workExecutorFactory = workExecutorFactory;
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
	public <M extends Message> void subscribe(SubscriptionConfiguration<M> configuration) {
        requireNonNull(configuration);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(configuration.getMessageBin().getName() + "-processor-%d")
                .build();
        WorkExecutor workExecutor = workExecutorFactory.newWorkExecutor(configuration.getMessageCapacity(), threadFactory);

		MessageConsumerSelector<M> consumerSelector = configuration.getMessageConsumers();
        if (finalFallbackMessageConsumerFactory != null) {
        	consumerSelector = MessageConsumerSelector.builder(consumerSelector)
					.addNextConsumer(finalFallbackReadCount, finalFallbackMessageConsumerFactory.create())
					.build();
		}

        ActiveSubscription<M> subscription = ActiveSubscription.start(
        		client,
				configuration.getMessageBin(),
				workExecutor,
				consumerSelector,
				configuration.getRetryDelayGenerator(),
				exceptionResolver,
				timingEventSupport
		);
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

	public static class Builder {

		private final Client client;

		private WorkExecutorFactory workExecutorFactory = ThreadPoolWorkExecutor::newPooledWorkExecutor;
		private ExceptionResolver exceptionResolver = ExceptionResolver.builder().build();

		private int finalFallbackReadCount;
		private FallbackMessageConsumerFactory finalFallbackMessageConsumerFactory;

		private Builder(Client client) {
			this.client = client;
		}

		public Builder withWorkExecutorFactory(WorkExecutorFactory workExecutorFactory) {
			this.workExecutorFactory = requireNonNull(workExecutorFactory);
			return this;
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
			return new Subscriber(client, workExecutorFactory, exceptionResolver, finalFallbackReadCount, finalFallbackMessageConsumerFactory);
		}
	}
}
