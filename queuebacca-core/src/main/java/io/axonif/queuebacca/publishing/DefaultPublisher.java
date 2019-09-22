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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.MessageSerializer;
import io.axonif.queuebacca.OutgoingEnvelope;
import io.axonif.queuebacca.publishing.events.PublishEventListener;
import io.axonif.queuebacca.publishing.events.PublishEventSupport;

/**
 * Publishes messages using to a specific {@link MessageBin}.
 */
final class DefaultPublisher implements Publisher {

    private final PublishEventSupport eventSupport = new PublishEventSupport();

    private final Client client;
    private final MessageSerializer messageSerializer;

    DefaultPublisher(Client client, MessageSerializer messageSerializer) {
        this.client = client;
        this.messageSerializer = messageSerializer;
    }

    @Override
    public <Message> PublishMessageResponse publish(PublishMessageRequest<Message> request) {
        requireNonNull(request);

        List<String> messageBodies = request.getMessages().stream()
                .map(m -> messageSerializer.toString(m, request.getMessageType()))
                .collect(Collectors.toList());

        Collection<OutgoingEnvelope> outgoingEnvelopes = client.sendMessages(request.getMessageBin(), messageBodies, request.getDeliveryDelay());

        List<PublishMessageResponse.PublishedMessage> publishedMessages = new ArrayList<>();
        for (OutgoingEnvelope outgoingEnvelope : outgoingEnvelopes) {
            publishedMessages.add(new PublishMessageResponse.PublishedMessage(outgoingEnvelope.getMessageId(), outgoingEnvelope.getMessageBody()));

            eventSupport.fireEvent(request.getMessageType(), outgoingEnvelope.getMessageId(), outgoingEnvelope.getMessageBody());
        }
        return new PublishMessageResponse(publishedMessages);
    }

    @Override
    public void addListener(PublishEventListener listener) {
        eventSupport.addListener(listener);
    }

    @Override
    public void addListener(Class<?> messageType, PublishEventListener listener) {
        eventSupport.addListener(messageType, listener);
    }

    @Override
    public void removeListener(PublishEventListener listener) {
        eventSupport.removeListener(listener);
    }

    @Override
    public void removeListener(Class<?> messageType, PublishEventListener listener) {
        eventSupport.removeListener(messageType, listener);
    }
}
