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

package io.axonif.queuebacca.json;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.axonif.queuebacca.exceptions.QueuebaccaException;
import io.axonif.queuebacca.MessageSerializer;

public class JacksonSerializer implements MessageSerializer {

    private final ObjectMapper mapper;

    public JacksonSerializer(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public <Message> String toString(Message object, Class<? extends Message> messageType) {
        if (object == null) throw new QueuebaccaException("Target cannot be null");
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to map to " + object + " to string", e);
        }
    }

    @Override
    public <Message> Message fromString(String json, Class<? extends Message> messageType) {
        if (json == null) throw new QueuebaccaException("String cannot be null");
        if (messageType == null) throw new QueuebaccaException("Class type cannot be null");
        try {
            return mapper.readerFor(messageType).readValue(json);
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to map from " + json + " to " + messageType.getName(), e);
        }
    }
}
