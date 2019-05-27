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

import java.util.Objects;

/**
 * Represents a specific location for message storage (queue, topic, etc.) within a message broker. The name is a generic
 * identifier for these locations.
 */
public final class MessageBin {

    private final String name;

    /**
     * Creates a new instance of a {@link MessageBin} with the provided name.
     *
     * @param name the name of the message bin
     */
    public MessageBin(String name) {
        this.name = requireNonNull(name);
    }

    /**
     * Gets the name of the {@link MessageBin}.
     *
     * @return the message bin name
     */
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageBin)) return false;
        MessageBin that = (MessageBin) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
