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
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * A {@link MessageConsumer} that allows for additional, processing of {@link Message Messages} using a chain
 * of responsibility.
 *
 * @param <M> the message type
 */
public final class 	ScopedMessageConsumer<M extends Message> implements MessageConsumer<M> {

	/**
	 * A processor to provide additional processing before or after a {@link Message} is consumed.
	 */
	public interface MessageScope {

		/**
		 * Perform processing using the provided {@link M message} and {@link MessageContext}. In order to continue the chain,
		 * a call to {@link MessageScopeChain#next()} is required. Failing to do so will result in
		 * the message being disposed of and considered successfully consumed, which may be the desired result.
		 *
		 * @param message the message being consumed
		 * @param messageContext the context of the message
		 * @param messageScopeChain the chain to continue processing to consumption
		 * @param <M> the message type
		 */
		<M> void wrap(M message, MessageContext messageContext, MessageScopeChain messageScopeChain);
	}

	/**
	 * The {@link MessageScope} chain used to continue invoking processors until the final call to the {@link MessageConsumer}.
	 */
	public interface MessageScopeChain {

		/**
		 * Calls the next {@link MessageScope}, or if there are none left, the {@link MessageConsumer}.
		 */
		void next();
	}

	private final MessageConsumer<M> consumer;
	private final List<MessageScope> messageScopes;

	/**
	 * Creates a new instance of a {@link ScopedMessageConsumer} with a {@link MessageConsumer} it will delegate to,
	 * and one or more {@link MessageScope MessageScopes}.
	 *
	 * @param consumer the consumer being decorated
	 * @param messageScope the first processor
	 * @param messageScopes any additional processors
	 */
	public ScopedMessageConsumer(MessageConsumer<M> consumer, MessageScope messageScope, MessageScope... messageScopes) {
		requireNonNull(messageScope);
		requireNonNull(messageScopes);

		this.consumer = requireNonNull(consumer);
		this.messageScopes = concat(of(messageScope), of(messageScopes)).collect(Collectors.toList());
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 *     This consumer will run the {@link Message} through a chain of {@link MessageScope MessageScopes} before
	 *     invoking it's {@link MessageConsumer}.
	 * </p>
	 */
	@Override
	public void consume(M message, MessageContext messageContext) {
		requireNonNull(message);
		requireNonNull(messageContext);

		ConcreteMessageScopeChain messageScopeChain = new ConcreteMessageScopeChain(new LinkedList<>(messageScopes), message, messageContext);
		messageScopeChain.next();
	}

	/**
	 * The {@link MessageScopeChain} implementation containing a queue of {@link MessageScope MessageScopes}.
	 */
	private final class ConcreteMessageScopeChain implements MessageScopeChain {

		private final Queue<MessageScope> messageScopes;
		private final M message;
		private final MessageContext messageContext;

		ConcreteMessageScopeChain(Queue<MessageScope> messageScopes, M message, MessageContext messageContext) {
			this.messageScopes = messageScopes;
			this.message = message;
			this.messageContext = messageContext;
		}

		@Override
		public void next() {
			requireNonNull(message);
			requireNonNull(messageContext);

			if (!messageScopes.isEmpty()) {
				MessageScope messageScope = messageScopes.poll();
				messageScope.wrap(message, messageContext, this);
			} else {
				consumer.consume(message, messageContext);
			}
		}
	}
}
