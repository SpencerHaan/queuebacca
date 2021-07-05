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

package dev.haan.queuebacca.consumers;

import static dev.haan.queuebacca.consumers.ScopedMessageConsumer.MessageScope;
import static dev.haan.queuebacca.consumers.ScopedMessageConsumer.MessageScopeChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.haan.queuebacca.consumers.ScopedMessageConsumer;
import org.junit.Test;

import dev.haan.queuebacca.Message;
import dev.haan.queuebacca.MessageConsumer;
import dev.haan.queuebacca.MessageContext;
import dev.haan.queuebacca.MessageResponse;

public class ScopedMessageConsumerTest {

	@Test(expected = NullPointerException.class)
	public void consume_nullMessage() {
		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>(MessageConsumer.basic(message -> {}), new TestMessageScope());
		messageConsumer.consume(null, new MessageContext("", 0, Instant.now(), "rawMessage"));
	}

	@Test(expected = NullPointerException.class)
	public void consume_nullContext() {
		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>(MessageConsumer.basic(message -> {}), new TestMessageScope());
		messageConsumer.consume(new TestMessage(), null);
	}

	@Test
	public void consume_scoped() {
		String expectedId = "messageId";

		AtomicBoolean called = new AtomicBoolean(false);
		TestMessageScope testMessageScope = new TestMessageScope();
		MessageConsumer<Message> innerConsumer = MessageConsumer.basic(message -> called.set(true));

		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>(innerConsumer, testMessageScope);
		messageConsumer.consume(new TestMessage(), new MessageContext(expectedId, 0, Instant.now(), "rawMessage"));

		assertTrue(called.get());
		assertEquals(expectedId, testMessageScope.messageId);
	}

	@Test
	public void consume_brokenChain() {
		String expectedId = "messageId";

		AtomicBoolean called = new AtomicBoolean(false);
		BrokenChainMessageScope testMessageScope = new BrokenChainMessageScope();
		MessageConsumer<Message> innerConsumer = MessageConsumer.basic(message -> called.set(true));

		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>(innerConsumer, testMessageScope);
		messageConsumer.consume(new TestMessage(), new MessageContext(expectedId, 0, Instant.now(), "rawMessage"));

		assertFalse(called.get());
		assertEquals(expectedId, testMessageScope.messageId);
	}

	@Test(expected = RuntimeException.class)
	public void consume_exceptionPassesThrough() {
		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>((message, context) -> {
			throw new RuntimeException();
		}, new TestMessageScope());
		messageConsumer.consume(new TestMessage(), new MessageContext("", 0, Instant.now(), "rawMessage"));
	}

	private static class TestMessage implements Message { }

	private static class TestMessageScope implements MessageScope {

		private String messageId;

		@Override
		public <M> MessageResponse wrap(M message, MessageContext messageContext, MessageScopeChain messageScopeChain) {
			MessageResponse response = messageScopeChain.next();
			messageId = messageContext.getMessageId();
			return response;
		}
	}

	private static class BrokenChainMessageScope implements MessageScope {

		private String messageId;

		@Override
		public <M> MessageResponse wrap(M message, MessageContext messageContext, MessageScopeChain messageScopeChain) {
			messageId = messageContext.getMessageId();
			return MessageResponse.TERMINATE;
		}
	}
}