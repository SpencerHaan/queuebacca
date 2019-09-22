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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestClient implements Client {

	private final Map<String, BlockingQueue<MessageWrapper>> storedMessages = new ConcurrentHashMap<>();

	public int getMessageCount(MessageBin messageBin) {
		return getQueue(messageBin).size();
	}

	public boolean containsMessage(MessageBin messageBin, String messageBody) {
		return getQueue(messageBin).stream().anyMatch(w -> w.getMessageBody().equals(messageBody));
	}

	@Override
	public OutgoingEnvelope sendMessage(MessageBin messageBin, String messageBody, int delay) {
		Instant availableBy = Instant.now().plusSeconds(delay);
		MessageWrapper messageWrapper = new MessageWrapper(messageBody, availableBy);
		getQueue(messageBin).offer(messageWrapper);
		return new OutgoingEnvelope(messageWrapper.getId(), messageWrapper.getMessageBody());
	}

	@Override
	public Collection<OutgoingEnvelope> sendMessages(MessageBin messageBin, Collection<String> messageBodies, int delay) {
		return messageBodies.stream()
				.map(m -> sendMessage(messageBin, m, delay))
				.collect(Collectors.toList());
	}

	@Override
	public Optional<IncomingEnvelope> retrieveMessage(MessageBin messageBin) {
		try {
			MessageWrapper messageWrapper = getQueue(messageBin).poll(1, TimeUnit.SECONDS);
			return Optional.ofNullable(messageWrapper)
					.map(w -> new IncomingEnvelope(w.getId(), "receipt", w.incrementCount(), Instant.now(), w.getMessageBody()));
		} catch (InterruptedException e) {
			return Optional.empty();
		}
	}


	@Override
	public Collection<IncomingEnvelope> retrieveMessages(MessageBin messageBin, int maxMessages) {
		Collection<IncomingEnvelope> envelopes = new ArrayList<>();

		Optional<IncomingEnvelope> messageWrapper;
		while (envelopes.size() < maxMessages && (messageWrapper = retrieveMessage(messageBin)).isPresent()) {
			envelopes.add(messageWrapper.get());
		}
		return envelopes;
	}

	@Override
	public void returnMessage(MessageBin messageBin, IncomingEnvelope incomingEnvelope, int delay) {
		Instant availableBy = Instant.now().plusSeconds(delay);
		MessageWrapper messageWrapper = new MessageWrapper(incomingEnvelope.getMessageId(), incomingEnvelope.getMessageBody(), availableBy);
		getQueue(messageBin).offer(messageWrapper);
	}

	@Override
	public void disposeMessage(MessageBin messageBin, IncomingEnvelope incomingEnvelope) {
		// Do nothing
	}

	private BlockingQueue<MessageWrapper> getQueue(MessageBin messageBin) {
		return storedMessages.computeIfAbsent(messageBin.getName(), s -> new PriorityBlockingQueue<>());
	}

	private static class MessageWrapper implements Comparable<MessageWrapper> {

		private final String id;
		private final String messageBody;
		private Instant availableBy;

		private int readCount = 0;

		MessageWrapper(String messageBody, Instant availableBy) {
			this(UUID.randomUUID().toString(), messageBody, availableBy);
		}

		MessageWrapper(String id, String messageBody, Instant availableBy) {
			this.id = requireNonNull(id);
			this.messageBody = requireNonNull(messageBody);
			this.availableBy = requireNonNull(availableBy);
		}

		String getId() {
			return id;
		}

		String getMessageBody() {
			return messageBody;
		}

		int incrementCount() {
			return ++readCount;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof MessageWrapper)) return false;
			MessageWrapper that = (MessageWrapper) o;
			return Objects.equals(id, that.id);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id);
		}

		@Override
		public int compareTo(MessageWrapper o) {
			return availableBy.compareTo(o.availableBy);
		}
	}
}
