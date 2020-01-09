/*
 * Copyright 2020 The Queuebacca Authors
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

package io.axonif.queuebacca.subscribing;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageConsumer;

class MessageConsumerSelector<M extends Message> {

    private final NavigableMap<Integer, MessageConsumer<M>> consumers;

    private MessageConsumerSelector(NavigableMap<Integer, MessageConsumer<M>> consumers) {
        this.consumers = consumers;
    }

    static <M extends Message> Builder<M> builder(MessageConsumer<M> firstMessageConsumer) {
        requireNonNull(firstMessageConsumer);

        NavigableMap<Integer, MessageConsumer<M>> consumers = new TreeMap<>();
        consumers.put(0, firstMessageConsumer);
        return new Builder<>(consumers);
    }

    static <M extends Message> Builder<M> builder(MessageConsumerSelector<M> existingSelector) {
        requireNonNull(existingSelector);
        return new Builder<>(new TreeMap<>(existingSelector.consumers));
    }

    MessageConsumer<M> select(int readCount) {
        return consumers.floorEntry(readCount).getValue();
    }

    int currentReadCountThreshold(int readCount) {
        return consumers.subMap(0, readCount).keySet().stream()
                .mapToInt(v -> v)
                .sum();
    }

    static class Builder<M extends Message> {

        private final NavigableMap<Integer, MessageConsumer<M>> consumers;

        private Builder(NavigableMap<Integer, MessageConsumer<M>> consumers) {
            this.consumers = consumers;
        }

        Builder<M> addNextConsumer(int readCountThreshold, MessageConsumer<M> messageConsumer) {
            consumers.put(consumers.lastKey() + readCountThreshold, messageConsumer);
            return this;
        }

        MessageConsumerSelector<M> build() {
            return new MessageConsumerSelector<>(Collections.unmodifiableNavigableMap(consumers));
        }
    }
}
