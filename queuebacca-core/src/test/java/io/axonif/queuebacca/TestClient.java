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

	private final Map<String, BlockingQueue<MessageWrapper<? extends Message>>> storedMessages = new ConcurrentHashMap<>();

	public int getMessageCount(MessageBin messageBin) {
		return getQueue(messageBin).size();
	}

	public <M extends Message> boolean containsMessage(MessageBin messageBin, M message) {
		return getQueue(messageBin).stream().anyMatch(w -> w.getMessage().equals(message));
	}

	@Override
	public <M extends Message> OutgoingEnvelope<M> sendMessage(MessageBin messageBin, M message, int delay) {
		Instant availableBy = Instant.now().plusSeconds(delay);
		MessageWrapper<M> messageWrapper = new MessageWrapper<>(message, availableBy);
		getQueue(messageBin).offer(messageWrapper);
		return new OutgoingEnvelope<>(messageWrapper.getId(), messageWrapper.getMessage());
	}

	@Override
	public <M extends Message> Collection<OutgoingEnvelope<M>> sendMessages(MessageBin messageBin, Collection<M> messages, int delay) {
		return messages.stream()
				.map(m -> sendMessage(messageBin, m, delay))
				.collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <M extends Message> Optional<IncomingEnvelope<M>> retrieveMessage(MessageBin messageBin) {
		try {
			MessageWrapper<M> messageWrapper = (MessageWrapper<M>) getQueue(messageBin).poll(1, TimeUnit.SECONDS);
			return Optional.ofNullable(messageWrapper)
					.map(w -> new IncomingEnvelope<>(w.getId(), "receipt", w.incrementCount(), Instant.now(), w.getMessage(), "not serialized"));
		} catch (InterruptedException e) {
			return Optional.empty();
		}
	}


	@Override
	public <M extends Message> Collection<IncomingEnvelope<M>> retrieveMessages(MessageBin messageBin, int maxMessages) {
		Collection<IncomingEnvelope<M>> envelopes = new ArrayList<>();

		Optional<IncomingEnvelope<M>> messageWrapper;
		while (envelopes.size() < maxMessages && (messageWrapper = retrieveMessage(messageBin)).isPresent()) {
			envelopes.add(messageWrapper.get());
		}
		return envelopes;
	}

	@Override
	public void returnMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope, int delay) {
		Instant availableBy = Instant.now().plusSeconds(delay);
		MessageWrapper<?> messageWrapper = new MessageWrapper<Message>(incomingEnvelope.getMessageId(), incomingEnvelope.getMessage(), availableBy);
		getQueue(messageBin).offer(messageWrapper);
	}

	@Override
	public void disposeMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope) {
		// Do nothing
	}

	private BlockingQueue<MessageWrapper<? extends Message>> getQueue(MessageBin messageBin) {
		return storedMessages.computeIfAbsent(messageBin.getName(), s -> new PriorityBlockingQueue<>());
	}

	private static class MessageWrapper<M extends Message> implements Comparable<MessageWrapper<M>> {

		private final String id;
		private final M message;
		private Instant availableBy;

		private int readCount = 0;

		public MessageWrapper(M message, Instant availableBy) {
			this(UUID.randomUUID().toString(), message, availableBy);
		}

		public MessageWrapper(String id, M message, Instant availableBy) {
			this.id = requireNonNull(id);
			this.message = requireNonNull(message);
			this.availableBy = requireNonNull(availableBy);
		}

		public String getId() {
			return id;
		}

		public M getMessage() {
			return message;
		}

		public int incrementCount() {
			return ++readCount;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof MessageWrapper)) return false;
			MessageWrapper<?> that = (MessageWrapper<?>) o;
			return Objects.equals(id, that.id);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id);
		}

		@Override
		public int compareTo(MessageWrapper<M> o) {
			return availableBy.compareTo(o.availableBy);
		}
	}
}
