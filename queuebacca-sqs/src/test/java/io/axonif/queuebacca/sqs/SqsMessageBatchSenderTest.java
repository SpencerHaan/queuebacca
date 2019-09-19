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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;
import org.mockito.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageResult;

import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.OutgoingEnvelope;

public class SqsMessageBatchSenderTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageBatchSenderTest.class);

	private AmazonSQS mockAmazonSQS = mock(AmazonSQS.class);
	private MessageSerializer serializer = new TestMessageSerializer();

	@Test
	public void send() {
		when(mockAmazonSQS.sendMessageBatch(Matchers.any())).thenAnswer(invocation -> {
			SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);
			Collection<SendMessageBatchResultEntry> successful = request.getEntries().stream()
					.map(e -> new SendMessageBatchResultEntry().withMessageId(UUID.randomUUID().toString()).withId(e.getId()))
					.collect(Collectors.toList());
			return new SendMessageBatchResult().withSuccessful(successful);
		});

		Set<TestMessage> expectedMessages = new TreeSet<>();
		SqsMessageBatchSender<TestMessage> batchSender = new SqsMessageBatchSender<>(mockAmazonSQS, serializer, LOGGER);
		for (int i = 0; i < 10; i++) {
			TestMessage message = new TestMessage("test-" + i);
			expectedMessages.add(message);
			batchSender.add(message);
		}

		Set<TestMessage> actualMessages = batchSender.send("fake-url", 0).stream()
				.map(OutgoingEnvelope::getMessage)
				.collect(Collectors.toCollection(TreeSet::new));

		assertEquals(expectedMessages, actualMessages);
	}

	@Test
	public void send_MessageRetry() {
		when(mockAmazonSQS.sendMessageBatch(Matchers.any())).thenAnswer(invocation -> {
			SendMessageBatchRequest request = invocation.getArgument(0, SendMessageBatchRequest.class);

			List<SendMessageBatchRequestEntry> entries = new ArrayList<>(request.getEntries());
			SendMessageBatchRequestEntry requestEntry = entries.remove(0);
			BatchResultErrorEntry failed = new BatchResultErrorEntry()
					.withId(requestEntry.getId())
					.withCode("B0RK")
					.withMessage("Borked")
					.withSenderFault(false);

			Collection<SendMessageBatchResultEntry> successful = entries.stream()
					.map(e -> new SendMessageBatchResultEntry().withMessageId(UUID.randomUUID().toString()).withId(e.getId()))
					.collect(Collectors.toList());
			return new SendMessageBatchResult()
					.withSuccessful(successful)
					.withFailed(failed);
		});
		when(mockAmazonSQS.sendMessage(Matchers.any())).then(i -> new SendMessageResult().withMessageId(UUID.randomUUID().toString()));

		Set<TestMessage> expectedMessages = new TreeSet<>();
		SqsMessageBatchSender<TestMessage> batchSender = new SqsMessageBatchSender<>(mockAmazonSQS, serializer, LOGGER);
		for (int i = 0; i < 10; i++) {
			TestMessage message = new TestMessage("test-" + i);
			expectedMessages.add(message);
			batchSender.add(message);
		}

		Set<TestMessage> actualMessages = batchSender.send("fake-url", 0).stream()
				.map(OutgoingEnvelope::getMessage)
				.collect(Collectors.toCollection(TreeSet::new));

		assertEquals(expectedMessages, actualMessages);
	}

	public static class TestMessage implements Message, Comparable<TestMessage> {

		private String value;

		private TestMessage(String value) {
			this.value = value;
		}

		@SuppressWarnings("unused")
		private TestMessage() {
			// Required by Jackson
		}

		@Override
		public int compareTo(TestMessage o) {
			return value.compareTo(o.value);
		}
	}

	// SPENCER Revisit this
	public static class TestMessageSerializer implements MessageSerializer {

		@Override
		public <M> String toString(M message, Class<M> messageClass) {
			if (message instanceof TestMessage) {
				return ((TestMessage) message).value;
			}
			throw new UnsupportedOperationException("Must be of type TestMessage");
		}

		@SuppressWarnings("unchecked")
		@Override
		public <M> M fromString(String body, Class<M> messageClass) {
			if (messageClass.equals(TestMessage.class)) {
				return (M) new TestMessage(body);
			}
			throw new UnsupportedOperationException("Must be of type TestMessage");
		}
	}
}