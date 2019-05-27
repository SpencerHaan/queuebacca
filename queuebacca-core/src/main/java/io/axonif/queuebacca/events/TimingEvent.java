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

package io.axonif.queuebacca.events;

import static java.util.Objects.requireNonNull;

import java.time.Instant;

import io.axonif.queuebacca.MessageBin;

/**
 * An event containing timing information for a specific {@link io.axonif.queuebacca.Message} from a {@link MessageBin}
 */
public class TimingEvent {

    private final MessageBin messageBin;
    private final Class<?> messageType;
    private final String messageId;
    private final Instant timestamp;
    private final long duration;

    public TimingEvent(MessageBin messageBin, Class<?> messageType, String messageId, Instant timestamp, long duration) {
        this.messageBin = requireNonNull(messageBin);
        this.messageType = requireNonNull(messageType);
        this.messageId = requireNonNull(messageId);
        this.timestamp = requireNonNull(timestamp);
        this.duration = duration;
    }

    public MessageBin getMessageBin() {
        return messageBin;
    }

    public Class<?> getMessageType() {
        return messageType;
    }

    public String getMessageId() {
        return messageId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public double getDuration() {
        return duration;
    }
}