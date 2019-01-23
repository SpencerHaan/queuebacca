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

import static java.util.Objects.requireNonNull;

import javax.security.auth.login.Configuration;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;

import io.axonif.queuebacca.MessageBin;

public class GcpManager {

    private final PublisherStub publisher;
    private final SubscriberStub subscriber;
    private final JacksonSerializer serializer;

    public GcpManager(PublisherStub publisher, SubscriberStub subscriber, JacksonSerializer serializer) {
        this.publisher = requireNonNull(publisher);
        this.subscriber = requireNonNull(subscriber);
        this.serializer = requireNonNull(serializer);
    }

    public void registerMessageBin(MessageBin messageBin, Configuration configuration) {
        ListSubscriptionsRequest listSubscriptionsRequest = ListSubscriptionsRequest.newBuilder()
                .
                .build();
        Topic topic = Topic.newBuilder()
                .setName("")
                .build();
        publisher.createTopicCallable()
                .call(topic);

        Subscription subscription = Subscription.newBuilder()
                .setName("")
                .build();
        subscriber.createSubscriptionCallable()
                .call(subscription);

    }

    public GcpClient newClient() {
        return new GcpClient(publisher, subscriber, serializer);
    }
}
