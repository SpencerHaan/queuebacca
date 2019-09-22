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

import java.util.Collection;
import java.util.Optional;

/**
 * The client that interacts with the underlying message broker for sending, retrieving, returning and disposing of messages.
 */
public interface Client {

    /**
     * Sends a single message to the provided {@link MessageBin} with an optional delay.
     *
     * @param messageBin the message bin to send the message to
     * @param messageBody the message
     * @param delay a delay of 0 or greater
     * @return a {@link OutgoingEnvelope} containing information of the sent message
     */
    OutgoingEnvelope sendMessage(MessageBin messageBin, String messageBody, int delay);

    /**
     * Sends a collection of messages to the provided {@link MessageBin} with an optional delay
     * that will be applied to all of them.
     *
     * @param messageBin the message bin to send the messages to
     * @param messageBodies the messages
     * @param delay a delay of 0 or greater
     * @return a collection of {@link OutgoingEnvelope OutgoingEnvelopes} containing information of the sent messages
     */
    Collection<OutgoingEnvelope> sendMessages(MessageBin messageBin, Collection<String> messageBodies, int delay);

    /**
     * Retrieves a single message from the provided {@link MessageBin}.
     *
     * @param messageBin the message bin to retrieve the message from
     * @return a message or {@link Optional#empty()} if no message could be retrieved.
     */
    default Optional<IncomingEnvelope> retrieveMessage(MessageBin messageBin) {
        return retrieveMessages(messageBin, 1).stream()
                .findFirst();
    }

    /**
     * Retrieves up to the max messages specified from the provided {@link MessageBin}.
     *
     * @param messageBin the message bin to retrieve the messages from
     * @param maxMessages the upper limit of messages to retrieve
     * @return zero or more messages up to the maximum
     */
    Collection<IncomingEnvelope> retrieveMessages(MessageBin messageBin, int maxMessages);

    /**
     * Returns a retrieved message to the provided {@link MessageBin}, giving it an optional delay.
     *
     * @param messageBin the message bin to return the message to
     * @param incomingEnvelope the previously retrieved {@link IncomingEnvelope}
     * @param delay a delay of 0 or greater
     */
    void returnMessage(MessageBin messageBin, IncomingEnvelope incomingEnvelope, int delay);

    /**
     * Disposes of a previously retrieved message from the provided {@link MessageBin}.
     *
     * @param messageBin the message bin to dispose the message from
     * @param incomingEnvelope the previously retrieved {@link IncomingEnvelope}
     */
    void disposeMessage(MessageBin messageBin, IncomingEnvelope incomingEnvelope);
}
