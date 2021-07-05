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

package dev.haan.queuebacca.sqs;

import static java.util.Objects.requireNonNull;

public final class SqsRedrivePolicy {

	public static SqsRedrivePolicy NONE = new SqsRedrivePolicy(0, null);

	private final int maxReceiveCount;
	private final String deadLetterTargetArn;

	private SqsRedrivePolicy(int maxReceiveCount, String deadLetterTargetArn) {
		this.maxReceiveCount = maxReceiveCount;
		this.deadLetterTargetArn = deadLetterTargetArn;
	}

	public static SqsRedrivePolicy create(int maxReceiveCount, String deadLetterTargetArn) {
		requireNonNull(deadLetterTargetArn);
		return new SqsRedrivePolicy(maxReceiveCount, deadLetterTargetArn);
	}

	public int getMaxReceiveCount() {
		return maxReceiveCount;
	}

	public String getDeadLetterTargetArn() {
		return deadLetterTargetArn;
	}
}
