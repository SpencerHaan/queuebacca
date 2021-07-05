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

import java.util.Collection;
import java.util.Optional;

/**
 * The client that interacts with the underlying message broker for sending, retrieving, returning and disposing of messages.
 */
public interface Client {

    /**
     * Sends a single {@link Message} to the provided {@link MessageBin} with an optional delay.
     *
     * @param messageBin the message bin to send the message to
     * @param message the message
     * @param delay a delay of 0 or greater
     * @param <M> the type of the message
     * @return a {@link OutgoingEnvelope} containing information of the sent message
     */
    <M extends Message> OutgoingEnvelope<M> sendMessage(MessageBin messageBin, M message, int delay); // SPENCER Add a default here for parity with retrieveMessage()

    /**
     * Sends a collection of {@link Message Messages} to the provided {@link MessageBin} with an optional delay
     * that will be applied to all of them.
     *
     * @param messageBin the message bin to send the messages to
     * @param messages the messages
     * @param delay a delay of 0 or greater
     * @param <M> the type of the message
     * @return a collection of {@link OutgoingEnvelope OutgoingEnvelopes} containing information of the sent messages
     */
    <M extends Message> Collection<OutgoingEnvelope<M>> sendMessages(MessageBin messageBin, Collection<M> messages, int delay);

    /**
     * Retrieves a single {@link Message} from the provided {@link MessageBin}.
     *
     * @param messageBin the message bin to retrieve the message from
     * @param <M> the type of the message
     * @return a {@link Message} or {@link Optional#empty()} if no message could be retrieved.
     */
    default <M extends Message> Optional<IncomingEnvelope<M>> retrieveMessage(MessageBin messageBin) throws InterruptedException {
        Collection<IncomingEnvelope<M>> messages = retrieveMessages(messageBin, 1);
        return messages.stream().findFirst();
    }

    /**
     * Retrieves up to the max {@link Message Messages} specified from the provided {@link MessageBin}.
     *
     * @param messageBin the message bin to retrieve the messages from
     * @param maxMessages the upper limit of messages to retrieve
     * @param <M> the type of the message
     * @return zero or more {@link Message Messages} up to the maximum
     */
    <M extends Message> Collection<IncomingEnvelope<M>> retrieveMessages(MessageBin messageBin, int maxMessages) throws InterruptedException;

    /**
     * Returns a retrieved {@link Message} to the provided {@link MessageBin}, giving it an optional delay.
     *
     * @param messageBin the message bin to return the message to
     * @param incomingEnvelope the previously retrieved {@link IncomingEnvelope}
     * @param delay a delay of 0 or greater
     */
    void returnMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope, int delay);

    /**
     * Disposes of a previously retrieved {@link Message} from the provided {@link MessageBin}.
     *
     * @param messageBin the message bin to dispose the message from
     * @param incomingEnvelope the previously retrieved {@link IncomingEnvelope}
     */
    void disposeMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope);
}
