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

package io.axonif.queuebacca.consumers;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.MessageConsumer;
import io.axonif.queuebacca.MessageContext;
import io.axonif.queuebacca.MessageResponse;
import io.axonif.queuebacca.exceptions.QueuebaccaConfigurationException;

/**
 * A {@link MessageConsumer} used to route a {@link Message} to a another, registered {@link MessageConsumer}.
 * This allows multiple message types to be published and subscribed for a single {@link MessageBin}.
 */
public final class RoutingMessageConsumer<Message> implements MessageConsumer<Message> {

    private final Map<Class<?>, Class<?>> routingMap = new ConcurrentHashMap<>();
    private final Map<Class<?>, MessageConsumer<? extends Message>> consumers;

    private RoutingMessageConsumer(Map<Class<?>, MessageConsumer<? extends Message>> consumers) {
        this.consumers = new HashMap<>(requireNonNull(consumers));
    }

    /**
     * Creates a new {@link RoutingMessageConsumer.Builder}.
     */
    public static <Message> Builder<Message> builder() {
        return new Builder<>();
    }

    /**
     * {@inheritDoc}
     * <p>
     *    This consumer will route the {@link Message} to the appropriately registered {@link MessageConsumer}.
     * </p>
     * @throws QueuebaccaConfigurationException if no consumer is available for the provided message type
     * @return a {@link MessageResponse} indicating how to handle the {@link Message} after it's been consumed
     */
    @Override
    public MessageResponse consume(Message message, MessageContext messageContext) {
        requireNonNull(message);
        requireNonNull(messageContext);

        MessageConsumer<Message> consumer = findConsumer(message.getClass())
                .orElseThrow(() -> new QueuebaccaConfigurationException("No consumer available for message '" +  message.getClass().getName() + "'"));
        return consumer.consume(message, messageContext);
    }

    @SuppressWarnings("unchecked")
    private Optional<MessageConsumer<Message>> findConsumer(Class<?> messageType) {
        Class<?> mappedMessageType = routingMap.computeIfAbsent(messageType, this::mapMessageType);
        return Optional.ofNullable(mappedMessageType)
                .map(mt -> (MessageConsumer<Message>) consumers.get(mt));
    }

    private Class<?> mapMessageType(Class<?> messageType) {
        if (messageType == null) {
            return null;
        } else if (consumers.containsKey(messageType)) {
            return messageType;
        } else {
            return Stream.of(messageType.getInterfaces())
                    .filter(consumers::containsKey)
                    .findFirst()
                    .orElseGet(() -> mapMessageType(messageType.getSuperclass()));
        }
    }

    /**
     * A builder for {@link RoutingMessageConsumer RoutingMessageConsumers}.
     *
     * @param <Message> the message type
     */
    public static class Builder<Message> {

        private final Map<Class<?>, MessageConsumer<? extends Message>> consumers = new HashMap<>();

        private Builder() {}

        /**
         * Registers a {@link Message} type to be routed to a given {@link MessageConsumer}.
         *
         * @param messageType the message type to route
         * @param consumer the consumer to route to
         * @param <T> the message type
         * @return this object for method chaining
         * @throws QueuebaccaConfigurationException if a consumer has already been registered for a given message type
         */
        public <T extends Message> Builder<Message> registerMessageRoute(Class<T> messageType, MessageConsumer<T> consumer) {
            requireNonNull(messageType);
            requireNonNull(consumer);

            String messageTypeName = messageType.getName();
            if (consumers.containsKey(messageType)) {
                throw new QueuebaccaConfigurationException("A message consumer for type '" + messageTypeName + "' has already been registered...");
            }

            consumers.put(messageType, consumer);
            return this;
        }

        /**
         * Creates an instance of {@link RoutingMessageConsumer}.
         *
         * @return the subscription configuration
         */
        public RoutingMessageConsumer<Message> build() {
            return new RoutingMessageConsumer<>(consumers);
        }
    }
}
