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

package dev.haan.queuebacca;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.Collection;

/**
 * Publishes messages using to a specific {@link MessageBin}. Messages can be "post-processed" using a {@link PostProcessor}.
 */
public final class Publisher {

    /**
     * A post-processor to provide additional processing once a {@link Message} has been published.
     */
    @FunctionalInterface
    public interface PostProcessor {

        /**
         * Perform processing using the provided {@link MessageContext} if needed.
         *
         * @param messageContext the context of the recently published message.
         */
        void process(MessageContext messageContext);
    }

    // Default post processor that does nothing
    private static PostProcessor NONE = context -> {};

    private final Client client;
    private final MessageBin messageBin;
    private final PostProcessor postProcessor;

    /**
     * Creates a new instance of {@link Publisher} with a {@link Client} and a {@link MessageBin}.
     *
     * @param client the client used to send messages
     * @param messageBin the message bin to send messages to
     */
    public Publisher(Client client, MessageBin messageBin) {
        this(client, messageBin, NONE);
    }

    /**
     * Creates a new instance of {@link Publisher} with a {@link Client}, a {@link MessageBin} and a {@link PostProcessor}.
     *
     * @param client the client used to send messages
     * @param messageBin the message bin to send messages to
     * @param postProcessor a post processor
     */
    public Publisher(Client client, MessageBin messageBin, PostProcessor postProcessor) {
        this.client = requireNonNull(client);
        this.messageBin = requireNonNull(messageBin);
        this.postProcessor = requireNonNull(postProcessor);
    }

    /**
     * Publish a single message.
     *
     * @param message the message
     * @throws NullPointerException if the message is null
     */
    public void publish(Message message) {
        publish(message, 0);
    }

    public void publish(Message message, PostProcessor postProcessor) {
        publish(message, 0, postProcessor);
    }

    /**
     * Publish a single message with a specified delay.
     *
     * @param message the message
     * @param delay a delay of 0 or greater
     * @throws NullPointerException if the message is null
     */
    public void publish(Message message, int delay) {
        publish(message, delay, NONE);
    }

    public void publish(Message message, int delay, PostProcessor postProcessor) {
        requireNonNull(message);

        OutgoingEnvelope<?> envelope = client.sendMessage(messageBin, message, delay);
        MessageContext messageContext = new MessageContext(envelope.getMessageId(), 0, Instant.now(), envelope.getRawMessage());
        postProcessor.process(messageContext);
        this.postProcessor.process(messageContext);
    }

    /**
     * Publishes a collection of messages.
     *
     * @param messages the messages
     * @throws NullPointerException if messages is null
     */
    public void publish(Collection<? extends Message> messages) {
        publish(messages, 0);
    }

    public void publish(Collection<? extends Message> messages, PostProcessor postProcessor) {
        publish(messages, 0, postProcessor);
    }

    /**
     * Publishes a collection of messages with a specified delay for each messagec.
     *
     * @param messages the messages
     * @param delay a delay of 0 or greater
     * @throws NullPointerException if messages is null
     */
    public void publish(Collection<? extends Message> messages, int delay) {
        publish(messages, delay, NONE);
    }

    public void publish(Collection<? extends Message> messages, int delay, PostProcessor postProcessor) {
        requireNonNull(messages);

        client.sendMessages(messageBin, messages, delay).stream()
                .map(e -> new MessageContext(e.getMessageId(), 0, Instant.now(), e.getRawMessage()))
                .forEach(c -> {
                    postProcessor.process(c);
                    this.postProcessor.process(c);
                });
    }
}
