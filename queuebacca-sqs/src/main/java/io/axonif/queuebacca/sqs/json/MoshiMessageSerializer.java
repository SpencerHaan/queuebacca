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

package io.axonif.queuebacca.sqs.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;

import io.axonif.queuebacca.exceptions.QueuebaccaException;
import io.axonif.queuebacca.sqs.MessageSerializer;

public class MoshiMessageSerializer implements MessageSerializer {

    private final Moshi moshi;

    public MoshiMessageSerializer(Moshi moshi) {
        this.moshi = requireNonNull(moshi);
    }

    public static MoshiMessageSerializer standard() {
        return new Builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> String toString(T object, Class<T> messageClass) {
        return moshi.adapter(messageClass).toJson(object);
    }

    @Override
    public <T> T fromString(String json, Class<T> messageClass) {
        try {
            return moshi.adapter(messageClass).fromJson(json);
        } catch (IOException e) {
            throw new QueuebaccaException("Failed to deserialize JSON string to {1}", messageClass);
        }
    }

    public static class Builder {

        private final Moshi.Builder builder = new Moshi.Builder();

        public <T> Builder add(Type type, JsonAdapter<T> jsonAdapter) {
            builder.add(type, jsonAdapter);
            return this;
        }

        public <T> Builder add(Type type, Class<? extends Annotation> annotation, JsonAdapter<T> jsonAdapter) {
            builder.add(type, annotation, jsonAdapter);
            return this;
        }

        public Builder add(JsonAdapter.Factory factory) {
            builder.add(factory);
            return this;
        }

        public Builder add(Object adapter) {
            builder.add(adapter);
            return this;
        }

        public MoshiMessageSerializer build() {
            return new MoshiMessageSerializer(builder.build());
        }
    }
}
