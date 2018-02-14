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

/**
 * Contains contextual information for the consumption of a {@link Message}.
 */
public final class Context {

	private final String messageId;
	private final int readCount;
	private final Instant firstReceived;

	/**
	 * Creates a new instance of a {@link Context} with the provided message id.
	 *
	 * @param messageId the unique id of the message
	 * @param readCount the number of times the message has been read
	 * @param firstReceived the first time the message was received as an {@link Instant}
	 */
	public Context(String messageId, int readCount, Instant firstReceived) {
		this.messageId = requireNonNull(messageId);
		this.readCount = readCount;
		this.firstReceived = requireNonNull(firstReceived);
	}

	/**
	 * The unique message id.
	 *
	 * @return the message id
	 */
	public String getMessageId() {
		return messageId;
	}

	public int getReadCount() {
		return readCount;
	}

	public Instant getFirstReceived() {
		return firstReceived;
	}
}
