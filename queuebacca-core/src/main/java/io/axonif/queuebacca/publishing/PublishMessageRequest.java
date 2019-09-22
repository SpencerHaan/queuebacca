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

package io.axonif.queuebacca.publishing;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

import java.util.Collection;
import java.util.stream.Collectors;

import io.axonif.queuebacca.MessageBin;

public class PublishMessageRequest<Message> {

    private final MessageBin messageBin;
    private final Collection<? extends Message> messages;
    private final Class<? extends Message> messageType;
    private final int deliveryDelay;

    private PublishMessageRequest(MessageBin messageBin, Collection<? extends Message> messages, Class<? extends Message> messageType, int deliveryDelay) {
        this.messages = messages;
        this.messageBin = messageBin;
        this.messageType = messageType;
        this.deliveryDelay = deliveryDelay;
    }

    @SafeVarargs
    public static <Message> Builder<Message> builder(MessageBin messageBin, Message message, Message... messages) {
        return builder(messageBin, concat(of(message), of(messages)).collect(Collectors.toList()));
    }

    public static <Message> Builder<Message> builder(MessageBin messageBin, Collection<? extends Message> messages) {
        return new Builder<>(messageBin, messages);
    }

    public Collection<? extends Message> getMessages() {
        return messages;
    }

    public MessageBin getMessageBin() {
        return messageBin;
    }

    public Class<? extends Message> getMessageType() {
        return messageType;
    }

    public int getDeliveryDelay() {
        return deliveryDelay;
    }

    public static class Builder<Message> {

        private final MessageBin messageBin;
        private final Collection<? extends Message> messages;

        private Class<? extends Message> messageType;
        private int deliveryDelay = 0;

        private Builder(MessageBin messageBin, Collection<? extends Message> messages) {
            this.messageBin = messageBin;
            this.messages = messages;
        }

        public Builder<Message> withMessageType(Class<? extends Message> messageType) {
            this.messageType = requireNonNull(messageType);
            return this;
        }

        public Builder<Message> withDeliveryDelay(int deliveryDelay) {
            this.deliveryDelay = deliveryDelay;
            return this;
        }

        public PublishMessageRequest<Message> build() {
            return new PublishMessageRequest<>(messageBin, messages, messageType, deliveryDelay);
        }
    }
}
