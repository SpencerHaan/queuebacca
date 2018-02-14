/*
 * Copyright 2018 The Queuebacca Authors
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

import io.axonif.queuebacca.retries.ConstantRetryDelay;

/**
 * Used as configuration for the creation of a subscription. A default capacity of {@link #DEFAULT_MESSAGE_CAPACITY} is
 * used along with a default {@link RetryDelayGenerator} of {@link ConstantRetryDelay}.
 *
 * @param <M> the message type
 */
public final class SubscriptionConfiguration<M extends Message> {

    private static final int DEFAULT_MESSAGE_CAPACITY = 10;
    private static final int DEFAULT_RETRY_DELAY = 5;

    private final MessageBin messageBin;
    private final MessageConsumer<M> messageConsumer;
    private final RetryDelayGenerator retryDelayGenerator;
    private final int messageCapacity;

    private SubscriptionConfiguration(MessageBin messageBin, MessageConsumer<M> messageConsumer, RetryDelayGenerator retryDelayGenerator, int messageCapacity) {
        this.messageBin = messageBin;
        this.messageConsumer = messageConsumer;
        this.retryDelayGenerator = retryDelayGenerator;
        this.messageCapacity = messageCapacity;
    }

    /**
     * Creates a new {@link SubscriptionConfiguration.Builder} with a required {@link MessageBin} and {@link MessageConsumer}.
     *
     * @param messageBin the subscriptions message bin
     * @param messageConsumer the subscriptions consumer
     */
    public static <M extends Message> Builder<M> builder(MessageBin messageBin, MessageConsumer<M> messageConsumer) {
        return new Builder<>(messageBin, messageConsumer);
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
    public MessageConsumer<M> getMessageConsumer() {
        return messageConsumer;
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

    /**
     * A builder for {@link SubscriptionConfiguration SubscriptionConfigurations}.
     *
     * @param <M> the message type
     */
    public static class Builder<M extends Message> {

        private final MessageBin messageBin;
        private final MessageConsumer<M> messageConsumer;

        private RetryDelayGenerator retryDelayGenerator = new ConstantRetryDelay(DEFAULT_RETRY_DELAY);
        private int messageCapacity = DEFAULT_MESSAGE_CAPACITY;

        private Builder(MessageBin messageBin, MessageConsumer<M> messageConsumer) {
            this.messageBin = requireNonNull(messageBin);
            this.messageConsumer = requireNonNull(messageConsumer);
        }

        /**
         * Sets the {@link RetryDelayGenerator} to be used by the subscription.
         *
         * @param retryDelayGenerator the retry delay generator
         * @return this object for method chaining
         */
        public Builder<M> withRetryDelayGenerator(RetryDelayGenerator retryDelayGenerator) {
            this.retryDelayGenerator = requireNonNull(retryDelayGenerator);
            return this;
        }

        /**
         * Sets the message capacity for the subscription. This is the number of messages a subscription can consume concurrently.
         *
         * @param messageCapacity the message capacity
         * @return this object for method chaining
         */
        public Builder<M> withMessageCapacity(int messageCapacity) {
            this.messageCapacity = messageCapacity;
            return this;
        }

        /**
         * Creates an instance of {@link SubscriptionConfiguration}.
         *
         * @return the subscription configuration
         */
        public SubscriptionConfiguration<M> build() {
            return new SubscriptionConfiguration<>(messageBin, messageConsumer, retryDelayGenerator, messageCapacity);
        }
    }
}
