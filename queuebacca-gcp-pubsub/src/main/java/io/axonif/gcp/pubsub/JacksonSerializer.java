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

import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;

import io.axonif.queuebacca.exceptions.QueuebaccaException;

public class JacksonSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JacksonSerializer.class);

    private final ObjectMapper mapper;

    public JacksonSerializer(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    public <T> String toJson(T object) {
        if (object == null) throw new QueuebaccaException("Target cannot be null");
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            String value = mapper.writeValueAsString(object);
            stopwatch.stop();
            LOGGER.debug("Jackson 'to' took {} for {}", stopwatch, object.getClass());

            return value;
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to map to " + object + " to string", e);
        }
    }

    public <T> T fromJson(String json, Class<T> clazz) {
        if (json == null) throw new QueuebaccaException("String cannot be null");
        if (clazz == null) throw new QueuebaccaException("Class type cannot be null");
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();

            T value = mapper.readerFor(clazz).readValue(json);

            stopwatch.stop();
            LOGGER.debug("Jackson 'from' took {} for {}", stopwatch, clazz);

            return value;
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to map from " + json + " to " + clazz.getName(), e);
        }
    }
}
