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
 * A container for an outgoing message.
 */
public final class OutgoingEnvelope {

    private final String messageId;
    private final String messageBody;

    /**
     * Creates a new instance of a {@link OutgoingEnvelope} with a message and it's unique id.
     *
     * @param messageId the message id
     * @param messageBody the message
     */
    public OutgoingEnvelope(String messageId, String messageBody) {
        this.messageId = requireNonNull(messageId);
        this.messageBody = requireNonNull(messageBody);
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
     * Gets the body of message.
     *
     * @return the message body
     */
    public String getMessageBody() {
        return messageBody;
    }
}
