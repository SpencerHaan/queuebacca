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

import java.util.NavigableMap;
import java.util.TreeMap;

import io.axonif.queuebacca.Subscriber.SubscriptionHarness;
import io.axonif.queuebacca.retries.ConstantRetryDelay;

/**
 * Used as configuration for the creation of a subscription. A default capacity of {@link #DEFAULT_MESSAGE_CAPACITY} is
 * used along with a default {@link RetryDelayGenerator} of {@link ConstantRetryDelay}.
 *
 * @param <Message> the message type
 */
public final class SubscriptionConfiguration<Message> {

    private static final int DEFAULT_MESSAGE_CAPACITY = 10;
    private static final int DEFAULT_RETRY_DELAY = 5;

    private final MessageBin messageBin;
    private final SubscriptionHarness<Message> subscriptionHarness;
    private final RetryDelayGenerator retryDelayGenerator;
    private final int messageCapacity;

    private SubscriptionConfiguration(MessageBin messageBin, SubscriptionHarness<Message> subscriptionHarness, RetryDelayGenerator retryDelayGenerator, int messageCapacity) {
        this.messageBin = messageBin;
        this.subscriptionHarness = subscriptionHarness;
        this.retryDelayGenerator = retryDelayGenerator;
        this.messageCapacity = messageCapacity;
    }

    /**
     * Creates a new {@link SubscriptionConfiguration.Builder} with a required {@link MessageBin} and {@link MessageConsumer}.
     *
     * @param messageBin the subscriptions message bin
     * @param messageConsumer the subscriptions consumer
     * @param <Message> the message type
     * @return the builder
     */
    public static <Message> Builder<Message> builder(MessageBin messageBin, MessageConsumer<Message> messageConsumer) {
        return new Builder<>(messageBin, new Subscriber.DefaultSubscriptionHarness<>(messageConsumer));
    }

    public static <Message> FallbackBuilder<Message> fallbackBuilder(MessageConsumer<Message> messageConsumer, int attempts) {
        return new FallbackBuilder<>(messageConsumer, attempts);
    }

    /**
     * Gets the currently configured {@link MessageBin}.
     *
     * @return the message bin
     */
    public MessageBin getMessageBin() {
        return messageBin;
    }

    /**
     * Gets the currently configured {@link MessageConsumer}.
     *
     * @return the message consumer
     */
    public SubscriptionHarness<Message> getSubscriptionHarness() {
        return subscriptionHarness;
    }

    /**
     * Gets the currently configured {@link RetryDelayGenerator}. Default is {@link ConstantRetryDelay}.
     *
     * @return the retry delay generator
     */
    public RetryDelayGenerator getRetryDelayGenerator() {
        return retryDelayGenerator;
    }

    /**
     * Gets the currently configured message capacity. Default is {@link #DEFAULT_MESSAGE_CAPACITY}.
     *
     * @return the message capacity
     */
    public int getMessageCapacity() {
        return messageCapacity;
    }

    public MessageSerializer getMessageSerializer() {
        return null; // TODO
    }

    public Class<? extends Message> getMessageType() {
        return null; // TODO
    }

    /**
     * A builder for {@link SubscriptionConfiguration SubscriptionConfigurations}.
     *
     * @param <Message> the message type
     */
    public static class Builder<Message> {

        private final MessageBin messageBin;
        private final SubscriptionHarness<Message> subscriptionHarness;

        private RetryDelayGenerator retryDelayGenerator = new ConstantRetryDelay(DEFAULT_RETRY_DELAY);
        private int messageCapacity = DEFAULT_MESSAGE_CAPACITY;

        private Builder(MessageBin messageBin, SubscriptionHarness<Message> subscriptionHarness) {
            this.messageBin = requireNonNull(messageBin);
            this.subscriptionHarness = requireNonNull(subscriptionHarness);
        }

        /**
         * Sets the {@link RetryDelayGenerator} to be used by the subscription.
         *
         * @param retryDelayGenerator the retry delay generator
         * @return this object for method chaining
         */
        public Builder<Message> withRetryDelayGenerator(RetryDelayGenerator retryDelayGenerator) {
            this.retryDelayGenerator = requireNonNull(retryDelayGenerator);
            return this;
        }

        /**
         * Sets the message capacity for the subscription. This is the number of messages a subscription can consume concurrently.
         *
         * @param messageCapacity the message capacity
         * @return this object for method chaining
         */
        public Builder<Message> withMessageCapacity(int messageCapacity) {
            this.messageCapacity = messageCapacity;
            return this;
        }

        /**
         * Creates an instance of {@link SubscriptionConfiguration}.
         *
         * @return the subscription configuration
         */
        public SubscriptionConfiguration<Message> build() {
            return new SubscriptionConfiguration<>(messageBin, subscriptionHarness, retryDelayGenerator, messageCapacity);
        }
    }

    public static class FallbackBuilder<Message> {

        private final NavigableMap<Integer, MessageConsumer<? extends Message>> consumers = new TreeMap<>();
        private int nextThreshold;

        <T extends Message> FallbackBuilder(MessageConsumer<T> consumer, int attempts) {
            consumers.put(0, consumer);
            nextThreshold = attempts;
        }

        public <T extends Message> FallbackBuilder<Message> thenUse(MessageConsumer<T> consumer, int attempts) {
            consumers.put(nextThreshold, consumer);
            nextThreshold += attempts;
            return this;
        }

        public <T extends Message> Builder<Message> finallyUse(MessageConsumer<T> consumer, MessageBin messageBin) {
            consumers.put(nextThreshold, consumer);
            return new Builder<>(messageBin, new Subscriber.FallbackSubscriptionHarness<>(consumers));
        }
    }
}
