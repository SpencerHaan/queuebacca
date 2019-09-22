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

import java.util.Collection;

import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.publishing.events.PublishEventListener;

public interface Publisher {

    int DEFAULT_DELAY = 0;

    <Message> PublishMessageResponse publish(PublishMessageRequest<Message> request);

    /**
     * Publish a single message.
     *
     * @param message the message
     * @throws NullPointerException if the message is null
     */
    default <Message> void publish(MessageBin messageBin, Message message) {
        publish(messageBin, message, DEFAULT_DELAY);
    }

    /**
     * Publish a single message with a specified delay.
     *
     * @param message the message
     * @param delay a delay of 0 or greater
     * @throws NullPointerException if the message is null
     */
    default <Message> void publish(MessageBin messageBin, Message message, int delay) {
        publish(PublishMessageRequest.builder(messageBin, message)
                .withDeliveryDelay(delay)
                .build());
    }

    /**
     * Publishes a collection of messages.
     *
     * @param messages the messages
     * @throws NullPointerException if messages is null
     */
    default <Message> void publish(MessageBin messageBin, Collection<? extends Message> messages) {
        publish(messageBin, messages, DEFAULT_DELAY);
    }

    /**
     * Publishes a collection of messages with a specified delay for each messagec.
     *
     * @param messages the messages
     * @param delay a delay of 0 or greater
     * @throws NullPointerException if messages is null
     */
    default <Message> void publish(MessageBin messageBin, Collection<? extends Message> messages, int delay) {
        publish(PublishMessageRequest.<Message>builder(messageBin, messages)
                .withDeliveryDelay(delay)
                .build());
    }

    void addListener(PublishEventListener listener);

    void addListener(Class<?> messageType, PublishEventListener listener);

    void removeListener(PublishEventListener listener);

    void removeListener(Class<?> messageType, PublishEventListener listener);
}
