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

package io.axonif.queuebacca.consumers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageConsumer;
import io.axonif.queuebacca.MessageContext;
import io.axonif.queuebacca.exceptions.QueuebaccaConfigurationException;

public class RoutingMessageConsumerTest {

	@Test(expected = QueuebaccaConfigurationException.class)
	public void consume_duplicateRoute() {
		RoutingMessageConsumer.builder()
				.registerMessageRoute(MessageType1.class, MessageConsumer.basic(message -> {}))
				.registerMessageRoute(MessageType1.class, MessageConsumer.basic(message -> {}));
	}

	@Test(expected = NullPointerException.class)
	public void consume_nullMessage() {
		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.build();
		messageConsumer.consume(null, new MessageContext("", 0, Instant.now(), "rawMessage"));
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
				.registerMessageRoute(Message.class, MessageConsumer.basic(message -> called.set(true)))
				.build();
		messageConsumer.consume(new MessageType1(), new MessageContext("", 0, Instant.now(), "rawMessage"));

		assertTrue(called.get());
	}

	@Test
	public void consume_singleRoute() {
		AtomicBoolean called1 = new AtomicBoolean(false);
		RoutingMessageConsumer<MessageType1> messageConsumer = RoutingMessageConsumer.<MessageType1>builder()
				.registerMessageRoute(MessageType1.class, MessageConsumer.basic(message -> called1.set(true)))
				.build();
		messageConsumer.consume(new MessageType1(), new MessageContext("", 0, Instant.now(), "rawMessage"));

		assertTrue(called1.get());
	}

	@Test
	public void consume_multipleRoutes() {
		String expectedMessage1Id = "message1Id";
		String expectedMessage2Id = "message2Id";

		AtomicReference<String> actualMessage1Id = new AtomicReference<>();
		AtomicReference<String> actualMessage2Id = new AtomicReference<>();

		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.registerMessageRoute(MessageType1.class, MessageConsumer.responseless((message, context) -> actualMessage1Id.set(context.getMessageId())))
				.registerMessageRoute(MessageType2.class, MessageConsumer.responseless((message, context) -> actualMessage2Id.set(context.getMessageId())))
				.build();
		messageConsumer.consume(new MessageType1(), new MessageContext(expectedMessage1Id, 0, Instant.now(), "rawMessage"));
		messageConsumer.consume(new MessageType2(), new MessageContext(expectedMessage2Id, 0, Instant.now(), "rawMessage"));

		assertEquals(expectedMessage1Id, actualMessage1Id.get());
		assertEquals(expectedMessage2Id, actualMessage2Id.get());
	}

	@Test(expected = QueuebaccaConfigurationException.class)
	public void consume_noRoute() {
		RoutingMessageConsumer<Message> messageConsumer = RoutingMessageConsumer.builder()
				.build();
		messageConsumer.consume(new MessageType1(), new MessageContext("", 0, Instant.now(), "rawMessage"));
	}

	private class MessageType1 implements Message { }

	private class MessageType2 implements Message { }
}