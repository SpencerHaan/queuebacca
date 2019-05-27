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

import java.util.function.Consumer;

/**
 * A consumer of {@link Message Messages}.
 *
 * @param <M> the message type
 */
public interface MessageConsumer<M extends Message> {

	static <M extends Message> MessageConsumer<M> ignoreContext(Consumer<M> consumer) {
		requireNonNull(consumer);
		return (message, context) -> consumer.accept(message);
	}

	/**
	 * Consumes the {@link M message}. A {@link Context} is provided to include additional information about the
	 * {@link Message}.
	 *
	 * @param message the message being consumed
	 * @param context the context
	 */
	void consume(M message, Context context);
}
