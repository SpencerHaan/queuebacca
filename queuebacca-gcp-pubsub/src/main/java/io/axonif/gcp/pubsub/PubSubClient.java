/*
 * Copyright 2018 The Queuebacca Authors
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

package io.axonif.gcp.pubsub;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.IncomingEnvelope;
import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.OutgoingEnvelope;

public class PubSubClient implements Client {

    private final PublisherStub publisher;
    private final SubscriberStub subscriber;
    private final JacksonSerializer jacksonSerializer;

    public PubSubClient(PublisherStub publisher, SubscriberStub subscriber, JacksonSerializer jacksonSerializer) {
        this.publisher = publisher;
        this.subscriber = subscriber;
        this.jacksonSerializer = jacksonSerializer;
    }

    @Override
    public <M extends Message> OutgoingEnvelope<M> sendMessage(MessageBin messageBin, M message, int delay) {
        return sendMessages(messageBin, Collections.singletonList(message), delay)
                .get(0);
    }

    @Override
    public <M extends Message> List<OutgoingEnvelope<M>> sendMessages(MessageBin messageBin, List<M> messages, int delay) {
        List<PubsubMessage> pubsubMessages = messages.stream()
                .map(m -> PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(jacksonSerializer.toJson(m))).build())
                .collect(Collectors.toList());
        PublishRequest publishRequest = PublishRequest.newBuilder()
                .setTopic(messageBin.getName())
                .addAllMessages(pubsubMessages)
                .build();
        PublishResponse call = publisher.publishCallable().call(publishRequest);
        return IntStream.range(0, messages.size())
                .mapToObj(i -> new OutgoingEnvelope<>(call.getMessageIds(i), messages.get(i)))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> Collection<IncomingEnvelope<M>> retrieveMessages(MessageBin messageBin, int maxMessages) {
        PullRequest pullRequest = PullRequest.newBuilder()
                .setMaxMessages(maxMessages)
                .setReturnImmediately(false)
                .setSubscription(messageBin.getName())
                .build();
        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
        return pullResponse.getReceivedMessagesList().stream()
            .map(receivedMessage -> new IncomingEnvelope(
                    receivedMessage.getMessage().getMessageId(),
                    receivedMessage.getAckId(),
                    0, // TODO
                    Instant.ofEpochSecond(receivedMessage.getMessage().getPublishTime().getSeconds()),
                    jacksonSerializer.fromJson(receivedMessage.getMessage().getData().toString(), Message.class)
            )).collect(Collectors.toList());
    }

    @Override
    public void returnMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope, int delay) {
        ModifyAckDeadlineRequest modifyAckDeadlineRequest = ModifyAckDeadlineRequest.newBuilder()
                .addAckIds(incomingEnvelope.getMessageId())
                .setSubscription(messageBin.getName())
                .setAckDeadlineSeconds(delay)
                .build();
        subscriber.modifyAckDeadlineCallable().call(modifyAckDeadlineRequest);
    }

    @Override
    public void disposeMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope) {
        AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
                .addAckIds(incomingEnvelope.getMessageId())
                .setSubscription(messageBin.getName())
                .build();
        subscriber.acknowledgeCallable().call(acknowledgeRequest);
    }
}
