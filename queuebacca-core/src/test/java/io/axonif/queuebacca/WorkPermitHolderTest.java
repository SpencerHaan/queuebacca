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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class WorkPermitHolderTest {

	@DataPoints
	public static int[] capacities() {
		return new int[] { 1, 2, 3, 4, 5 };
	}

	@Test(expected = IllegalArgumentException.class)
	public void new_capacityLessThanOrEqualTo0() {
		new WorkPermitHolder(0);
	}

	@Test
	public void acquire() throws InterruptedException {
		WorkPermitHolder permitHolder = new WorkPermitHolder(1);
		AtomicInteger threadCounter = new AtomicInteger(0);

		int totalThreads = 10;
		ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);

		for (int i = 1; i <= totalThreads; i++) {
			WorkPermitHolder.Permit permit = permitHolder.acquire();
			int expectedCount = i;
			executorService.submit(() -> {
				try {
					assertEquals(expectedCount, threadCounter.getAndIncrement());
				} finally {
					permit.release();
				}
			});
		}

		WorkPermitHolder.Permit permit = permitHolder.acquire();
		try {
			assertEquals(totalThreads, threadCounter.get());
		} finally {
			permit.release();
			executorService.shutdown();
		}
	}

	@Test
	public void tryAcquire() {
		WorkPermitHolder permitHolder = new WorkPermitHolder(1);

		Optional<WorkPermitHolder.Permit> permit = permitHolder.tryAcquire();
		assertTrue(permit.isPresent());
	}

	@Test
	public void tryAcquire_Exhausted() {
		WorkPermitHolder permitHolder = new WorkPermitHolder(1);

		Optional<WorkPermitHolder.Permit> permit1 = permitHolder.tryAcquire();
		assertTrue(permit1.isPresent());

		Optional<WorkPermitHolder.Permit> permit2 = permitHolder.tryAcquire();
		assertFalse(permit2.isPresent());

		permit1.ifPresent(WorkPermitHolder.Permit::release);
		permit2 = permitHolder.tryAcquire();
		assertTrue(permit2.isPresent());
	}

	@Theory
	public void acquireAvailable(int capacity) throws InterruptedException {
		WorkPermitHolder permitHolder = new WorkPermitHolder(capacity);
		Collection<WorkPermitHolder.Permit> permits = permitHolder.acquireAvailable();

		assertEquals(capacity, permits.size());
	}
}