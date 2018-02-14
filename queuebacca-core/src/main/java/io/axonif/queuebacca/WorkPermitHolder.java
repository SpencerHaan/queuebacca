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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * Issues {@link Permit Permits} from a fixed number of available permits.
 */
public final class WorkPermitHolder {

	private final BlockingQueue<Permit> issuedPermits;

	/**
	 * Creates a new instance of a {@link WorkPermitHolder} with the specified capacity.
	 *
	 * @param capacity number of available permits
	 * @throws IllegalArgumentException if capacity is 0 or less
	 */
	public WorkPermitHolder(int capacity) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("capacity must be greater than 0");
		}
		this.issuedPermits = new ArrayBlockingQueue<>(capacity, true);
	}

	/**
	 * Acquires a permit, blocking until one is available.
	 *
	 * @return the acquired permit
	 * @throws InterruptedException if the acquisition is interrupted
	 */
	public Permit acquire() throws InterruptedException {
		Permit permit = newPermit();
		issuedPermits.put(permit);
		return permit;
	}

	/**
	 * Attempts to acquire a permit, returning an empty {@link Optional} if it cannot.
	 *
	 * @return the acquired permit or {@link Optional#empty()}
	 */
	public Optional<Permit> tryAcquire() {
		Permit permit = newPermit();
		return issuedPermits.offer(permit)
				? Optional.of(permit)
				: Optional.empty();
	}

	/**
	 * Attempts to acquire all available permits, blocking until at least one is available.
	 *
	 * @return all acquired permits
	 * @throws InterruptedException if the acquisition is interrupted
	 */
	public Collection<Permit> acquireAvailable() throws InterruptedException {
		Permit permit = acquire();

		List<Permit> acquiredPermits = new LinkedList<>();
		acquiredPermits.add(permit);

		Optional<Permit> additionalPermit;
		do {
			additionalPermit = tryAcquire();
			additionalPermit.ifPresent(acquiredPermits::add);
		} while (additionalPermit.isPresent());
		return acquiredPermits;
	}

	private Permit newPermit() {
		return new Permit(issuedPermits::remove);
	}

	/**
	 * A work permit that represents a single unit of work as a finite resource. Used for concurrency control with the intention
	 * that the acquirer of the permit can pass it off to something else to performing the work.
	 */
	public static class Permit {

		private final Consumer<Permit> release;

		private Permit(Consumer<Permit> release) {
			this.release = release;
		}

		/**
		 * Releases the permit, allowing the permit to be reissued.
		 */
		public void release() {
			release.accept(this);
		}
	}
}
