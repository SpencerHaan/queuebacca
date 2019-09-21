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

package io.axonif.queuebacca.sqs;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.axonif.queuebacca.exceptions.QueuebaccaException;
import io.axonif.queuebacca.util.MessageSerializer;

public class JacksonSerializer implements MessageSerializer {

    private final ObjectMapper mapper;

    public JacksonSerializer(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public <T> String toString(T object) {
        if (object == null) throw new QueuebaccaException("Target cannot be null");
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to map to " + object + " to string", e);
        }
    }

    @Override
    public <T> T fromString(String json, Class<T> clazz) {
        if (json == null) throw new QueuebaccaException("String cannot be null");
        if (clazz == null) throw new QueuebaccaException("Class type cannot be null");
        try {
            return mapper.readerFor(clazz).readValue(json);
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to map from " + json + " to " + clazz.getName(), e);
        }
    }
}
