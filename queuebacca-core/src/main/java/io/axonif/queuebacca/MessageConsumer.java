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

package io.axonif.queuebacca;

import static java.util.Objects.requireNonNull;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A consumer of {@link Message Messages}.
 *
 * @param <M> the message type
 */
public interface MessageConsumer<M extends Message> {

	/**
	 * A convenience wrapper for a {@link MessageConsumer} that by default will always respond with {@link MessageResponse#CONSUMED}.
	 *
	 * @param consumer a simple {@link BiConsumer} that will provide the {@link Message} and {@link MessageContext} but doesn't require a {@link MessageResponse}
	 * @param <M> the message type
	 * @return the {@link BiConsumer} parameter wrapped as a {@link MessageConsumer}
	 */
	static <M extends Message> MessageConsumer<M> responseless(BiConsumer<M, MessageContext> consumer) {
		requireNonNull(consumer);
		return (message, context) -> {
			consumer.accept(message, context);
			return MessageResponse.CONSUMED;
		};
	}

	/**
	 * A convenience wrapper for a {@link MessageConsumer} that allows for a {@link MessageResponse} to be returned but doesn't require the {@link MessageContext}
	 * parameter.
	 *
	 * @param function a simple {@link Function} that will provide the {@link Message} and return a {@link MessageResponse}
	 * @param <M> the message type
	 * @return the {@link Function} parameter wrapped as a {@link MessageConsumer}
	 */
	static <M extends Message> MessageConsumer<M> contextless(Function<M, MessageResponse> function) {
		requireNonNull(function);
		return (message, context) -> function.apply(message);
	}

	/**
	 * A convenience wrapper for a {@link MessageConsumer} that by default will always respond with {@link MessageResponse#CONSUMED} and does't require the
	 * {@link MessageContext} parameter.
	 *
	 * @param consumer a simple {@link Consumer} that will provide the {@link Message} but doesn't provide a {@link MessageContext} and doesn't require a {@link MessageResponse}
	 * @param <M> the message type
	 * @return the {@link Consumer} parameter wrapped as a {@link MessageConsumer}
	 */
	static <M extends Message> MessageConsumer<M> basic(Consumer<M> consumer) {
		requireNonNull(consumer);
		return (message, context) -> {
			consumer.accept(message);
			return MessageResponse.CONSUMED;
		};
	}

	/**
	 * Consumes the {@link M message}. A {@link MessageContext} is provided to include additional information about the
	 * {@link Message}.
	 *  @param message the message being consumed
	 * @param messageContext the context
     * @return a {@link MessageResponse} indicating how to handle the {@link Message} after it's been consumed
     */
	MessageResponse consume(M message, MessageContext messageContext);
}
