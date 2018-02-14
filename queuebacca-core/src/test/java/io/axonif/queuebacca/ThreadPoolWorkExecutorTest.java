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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import io.axonif.queuebacca.exceptions.QueuebaccaException;

public class ThreadPoolWorkExecutorTest {

	@Test
	public void submitWorkOrders() throws InterruptedException {
		int expectedCapacity = 10;

		ThreadPoolWorkExecutor workerPool = ThreadPoolWorkExecutor.newPooledWorkExecutor(expectedCapacity, Thread::new);

		AtomicInteger timesPerformed = new AtomicInteger(0);
		CountDownLatch countDownLatch = new CountDownLatch(expectedCapacity);
		ThreadPoolWorkExecutor.WorkOrder testWorkOrder = () -> {
			timesPerformed.incrementAndGet();
			countDownLatch.countDown();
		};

		workerPool.submitWorkOrders(capacity -> IntStream.range(0, capacity)
				.mapToObj(i -> testWorkOrder)
				.collect(Collectors.toList()));

		countDownLatch.await();
		assertEquals(expectedCapacity, timesPerformed.get());

		workerPool.shutdownNow();
	}

	@Test(expected = QueuebaccaException.class)
	public void submitWorkOrders_ExceedsCapacity() throws InterruptedException {
		int expectedCapacity = 10;

		ThreadPoolWorkExecutor workerPool = ThreadPoolWorkExecutor.newPooledWorkExecutor(expectedCapacity, Thread::new);

		workerPool.submitWorkOrders(capacity -> IntStream.range(0, capacity + 1)
				.mapToObj(i -> (ThreadPoolWorkExecutor.WorkOrder) () -> {})
				.collect(Collectors.toList()));

		workerPool.shutdownNow();
	}
}