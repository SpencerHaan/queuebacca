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

package io.axonif.queuebacca;

import static io.axonif.queuebacca.ScopedMessageConsumer.MessageScope;
import static io.axonif.queuebacca.ScopedMessageConsumer.MessageScopeChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class ScopedMessageConsumerTest {

	@Test(expected = NullPointerException.class)
	public void consume_nullMessage() {
		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>((message, context) -> {}, new TestMessageScope());
		messageConsumer.consume(null, new Context("", 0, Instant.now()));
	}

	@Test(expected = NullPointerException.class)
	public void consume_nullContext() {
		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>((message, context) -> {}, new TestMessageScope());
		messageConsumer.consume(new TestMessage(), null);
	}

	@Test
	public void consume_scoped() {
		String expectedId = "messageId";

		AtomicBoolean called = new AtomicBoolean(false);
		TestMessageScope testMessageScope = new TestMessageScope();
		MessageConsumer<Message> innerConsumer = (message, context) -> called.set(true);

		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>(innerConsumer, testMessageScope);
		messageConsumer.consume(new TestMessage(), new Context(expectedId, 0, Instant.now()));

		assertTrue(called.get());
		assertEquals(expectedId, testMessageScope.messageId);
	}

	@Test
	public void consume_brokenChain() {
		String expectedId = "messageId";

		AtomicBoolean called = new AtomicBoolean(false);
		BrokenChainMessageScope testMessageScope = new BrokenChainMessageScope();
		MessageConsumer<Message> innerConsumer = (message, context) -> called.set(true);

		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>(innerConsumer, testMessageScope);
		messageConsumer.consume(new TestMessage(), new Context(expectedId, 0, Instant.now()));

		assertFalse(called.get());
		assertEquals(expectedId, testMessageScope.messageId);
	}

	@Test(expected = RuntimeException.class)
	public void consume_exceptionPassesThrough() {
		ScopedMessageConsumer<Message> messageConsumer = new ScopedMessageConsumer<>((message, context) -> {
			throw new RuntimeException();
		}, new TestMessageScope());
		messageConsumer.consume(new TestMessage(), new Context("", 0, Instant.now()));
	}

	private class TestMessage implements Message { }

	private class TestMessageScope implements MessageScope {

		private String messageId;

		@Override
		public <M> void wrap(M message, Context context, MessageScopeChain messageScopeChain) {
			messageScopeChain.next();
			messageId = context.getMessageId();
		}
	}

	private class BrokenChainMessageScope implements MessageScope {

		private String messageId;

		@Override
		public <M> void wrap(M message, Context context, MessageScopeChain messageScopeChain) {
			messageId = context.getMessageId();
		}
	}
}