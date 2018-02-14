/*
 * Copyright 2018 The QueueBacca Authors
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

/**
 * A container for an incoming {@link M message}.
 *
 * @param <M> the message type
 */
public final class IncomingEnvelope<M extends Message> {

    private final String messageId;
    private final String receipt;
    private final int readCount;
    private final Instant firstReceived;
    private final M message;

    /**
     * Creates a new instance of a {@link IncomingEnvelope} with a {@link M message}, it's unique id, a receipt for
     * it's retrieval, and it's current read count.
     *  @param messageId the message id
     * @param receipt a receipt unique to this envelope
     * @param readCount the number of times the message has been read
     * @param firstReceived the first time the message was received as an {@link Instant}
     * @param message the message
     */
    public IncomingEnvelope(String messageId, String receipt, int readCount, Instant firstReceived, M message) {
        this.messageId = requireNonNull(messageId);
        this.receipt = requireNonNull(receipt);
        this.readCount = readCount; // SPENCER protect against negatives
        this.firstReceived = requireNonNull(firstReceived);
        this.message = requireNonNull(message);
    }

    /**
     * Gets the messages id.
     *
     * @return the message id
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Gets the envelopes receipt.
     *
     * @return the receipt
     */
    public String getReceipt() {
        return receipt;
    }

    /**
     * Gets the number of times a message has been read.
     *
     * @return the read count
     */
    public int getReadCount() {
        return readCount;
    }

    public Instant getFirstReceived() {
        return firstReceived;
    }

    /**
     * Gets the {@link M message}.
     *
     * @return the message
     */
    public M getMessage() {
        return message;
    }

}
