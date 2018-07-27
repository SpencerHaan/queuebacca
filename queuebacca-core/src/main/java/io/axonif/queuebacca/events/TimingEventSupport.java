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

package io.axonif.queuebacca.events;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import io.axonif.queuebacca.MessageBin;

/**
 * Handles registration of {@link TimingEventListener TimingEventListeners} and firing {@link TimingEvent TimingEvents} for all registered listeners
 */
public class TimingEventSupport {

    private final List<TimingEventListener> timingEventListeners = new ArrayList<>();

    public void addListener(TimingEventListener timingEventListener) {
        requireNonNull(timingEventListener);

        timingEventListeners.add(timingEventListener);
    }

    public void removeListener(TimingEventListener timingEventListener) {
        requireNonNull(timingEventListener);

        timingEventListeners.remove(timingEventListener);
    }

    public void fireEvent(MessageBin messageBin, Class<?> messageType, String messageId, long duration) {
        TimingEvent timingEvent = new TimingEvent(messageBin, messageType, messageId, Instant.now(), duration);
        timingEventListeners.forEach(listener -> listener.handleTimingEvent(timingEvent));
    }
}
