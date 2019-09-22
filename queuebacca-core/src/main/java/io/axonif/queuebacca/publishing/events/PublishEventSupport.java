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

package io.axonif.queuebacca.publishing.events;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axonif.queuebacca.events.TimingEvent;

/**
 * Handles registration of {@link PublishEventListener TimingEventListeners} and firing {@link TimingEvent TimingEvents} for all registered listeners
 */
public class PublishEventSupport {

    private final Map<Class<?>, List<PublishEventListener>> contextualPublishEventListeners = new HashMap<>();
    private final List<PublishEventListener> publishEventListeners = new ArrayList<>();

    public void addListener(PublishEventListener listener) {
        requireNonNull(listener);

        publishEventListeners.add(listener);
    }

    public void addListener(Class<?> messageType, PublishEventListener listener) {
        requireNonNull(listener);

        contextualPublishEventListeners.computeIfAbsent(messageType, c -> new ArrayList<>())
                .add(listener);
    }

    public void removeListener(PublishEventListener listener) {
        requireNonNull(listener);

        publishEventListeners.remove(listener);
    }

    public void removeListener(Class<?> messageType, PublishEventListener listener) {
        requireNonNull(listener);

        contextualPublishEventListeners.computeIfAbsent(messageType, c -> new ArrayList<>())
                .remove(listener);
    }

    public void fireEvent(Class<?> messageType, String messageId, String messageBody) {
        MessagePublishedEvent event = new MessagePublishedEvent(messageId, messageBody);
        if (contextualPublishEventListeners.containsKey(messageType)) {
            contextualPublishEventListeners.get(messageType).forEach(listener -> listener.handleMessagePublishedEvent(event));
        }
        publishEventListeners.forEach(listener -> listener.handleMessagePublishedEvent(event));
    }
}
