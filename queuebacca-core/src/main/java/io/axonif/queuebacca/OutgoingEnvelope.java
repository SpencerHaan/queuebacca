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

/**
 * A container for an outgoing {@link M message}.
 *
 * @param <M> the message type
 */
public final class OutgoingEnvelope<M extends Message> {

    private final String messageId;
    private final M message;

    /**
     * Creates a new instance of a {@link OutgoingEnvelope} with a {@link M message} and it's unique id.
     *
     * @param messageId the message id
     * @param message the message
     */
    public OutgoingEnvelope(String messageId, M message) {
        this.messageId = requireNonNull(messageId);
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
     * Gets the {@link M message}.
     *
     * @return the message
     */
    public M getMessage() {
        return message;
    }
}
