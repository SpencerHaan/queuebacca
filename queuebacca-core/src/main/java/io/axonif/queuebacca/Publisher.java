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
         * Perform processing using the provided {@link Context} if needed.
         *
         * @param context the context of the recently published message.
         */
        void process(Context context);
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

    /**
     * Publish a single message with a specified delay.
     *
     * @param message the message
     * @param delay a delay of 0 or greater
     * @throws NullPointerException if the message is null
     */
    public void publish(Message message, int delay) {
        requireNonNull(message);

        OutgoingEnvelope<?> envelope = client.sendMessage(messageBin, message, delay);
        postProcessor.process(new Context(envelope.getMessageId(), 0, Instant.now()));
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

    /**
     * Publishes a collection of messages with a specified delay for each messagec.
     *
     * @param messages the messages
     * @param delay a delay of 0 or greater
     * @throws NullPointerException if messages is null
     */
    public void publish(Collection<? extends Message> messages, int delay) {
        requireNonNull(messages);

        client.sendMessages(messageBin, messages, delay).stream()
                .map(e -> new Context(e.getMessageId(), 0, Instant.now()))
                .forEach(postProcessor::process);
    }
}
