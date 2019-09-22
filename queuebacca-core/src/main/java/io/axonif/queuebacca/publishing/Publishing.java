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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.MessageSerializer;
import io.axonif.queuebacca.json.JacksonSerializer;

public final class Publishing {

    private Publishing() {
        // Static factory
    }

    public static Builder publisher(Client client) {
        return new Publishing.Builder(client);
    }

    public static class Builder {

        private final Client client;

        private MessageSerializer messageSerializer = new JacksonSerializer(new ObjectMapper());

        private Builder(Client client) {
            this.client = requireNonNull(client);
        }

        public Builder withMessageSerializer(MessageSerializer messageSerializer) {
            this.messageSerializer = requireNonNull(messageSerializer);
            return this;
        }

        public Publisher build() {
            return new DefaultPublisher(client, messageSerializer);
        }
    }
}
