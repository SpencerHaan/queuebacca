/*
 * Copyright 2018 The QueueBacca Authors
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.axonif.queuebacca.exceptions.QueueBaccaConfigurationException;

public class RoutingMessageConsumerTest {

	@Test(expected = QueueBaccaConfigurationException.class)
	public void consume_duplicateRoute() {
		RoutingMessageConsumer.builder()
				.registerMessageRoute(MessageType1.class, (message, context) -> {})
				.registerMessageRoute(MessageType1.class, (message, context) -> {});
	}

	@Test(expected = NullPointerException.class)
	public void consume_nullMessage() {
		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.build();
		messageConsumer.consume(null, new Context("", 0, Instant.now()));
	}

	@Test(expected = NullPointerException.class)
	public void consume_nullContext() {
		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.build();
		messageConsumer.consume(new MessageType1(), null);
	}

	@Test
	public void consume_inheritanceFallbackRoute() {
		AtomicBoolean called = new AtomicBoolean(false);
		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.registerMessageRoute(Message.class, (message, context) -> called.set(true))
				.build();
		messageConsumer.consume(new MessageType1(), new Context("", 0, Instant.now()));

		assertTrue(called.get());
	}

	@Test
	public void consume_singleRoute() {
		AtomicBoolean called1 = new AtomicBoolean(false);
		RoutingMessageConsumer<MessageType1> messageConsumer = RoutingMessageConsumer.<MessageType1>builder()
				.registerMessageRoute(MessageType1.class, (message, context) -> called1.set(true))
				.build();
		messageConsumer.consume(new MessageType1(), new Context("", 0, Instant.now()));

		assertTrue(called1.get());
	}

	@Test
	public void consume_multipleRoutes() {
		String expectedMessage1Id = "message1Id";
		String expectedMessage2Id = "message2Id";

		AtomicReference<String> actualMessage1Id = new AtomicReference<>();
		AtomicReference<String> actualMessage2Id = new AtomicReference<>();

		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.registerMessageRoute(MessageType1.class, (message, context) -> actualMessage1Id.set(context.getMessageId()))
				.registerMessageRoute(MessageType2.class, (message, context) -> actualMessage2Id.set(context.getMessageId()))
				.build();
		messageConsumer.consume(new MessageType1(), new Context(expectedMessage1Id, 0, Instant.now()));
		messageConsumer.consume(new MessageType2(), new Context(expectedMessage2Id, 0, Instant.now()));

		assertEquals(expectedMessage1Id, actualMessage1Id.get());
		assertEquals(expectedMessage2Id, actualMessage2Id.get());
	}

	@Test(expected = QueueBaccaConfigurationException.class)
	public void consume_noRoute() {
		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.build();
		messageConsumer.consume(new MessageType1(), new Context("", 0, Instant.now()));
	}

	private class MessageType1 implements Message { }

	private class MessageType2 implements Message { }
}