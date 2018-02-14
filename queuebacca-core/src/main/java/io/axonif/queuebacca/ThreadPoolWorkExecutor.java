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

import static io.axonif.queuebacca.WorkPermitHolder.Permit;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.axonif.queuebacca.exceptions.QueueBaccaException;

/**
 * A pooled {@link WorkExecutor} distributes it's work using a thread pool.
 */
public final class ThreadPoolWorkExecutor implements WorkExecutor {

    private final ThreadPoolExecutor pooledExecutor;
    private final WorkPermitHolder permitHolder;

    private ThreadPoolWorkExecutor(ThreadPoolExecutor pooledExecutor, WorkPermitHolder permitHolder) {
        this.pooledExecutor = pooledExecutor;
        this.permitHolder = permitHolder;
    }

    /**
     * Starts a {@link ThreadPoolWorkExecutor} with the provided capacity and {@link ThreadFactory}.
     *
     * @param capacity the number of available worker threads
     * @param threadFactory a factory for creating threads
     * @return the new thread pool work executor in a running state
     */
    public static ThreadPoolWorkExecutor newPooledWorkExecutor(int capacity, ThreadFactory threadFactory) {
        requireNonNull(threadFactory);

        int idleCapacity = capacity / 2 + capacity % 2; // integer safe division, rounding up
        int maxCapacity = capacity * 2; // provides a buffer if permit holder and executor don't perfectly match up
        ThreadPoolExecutor messageWorkerPool = new ThreadPoolExecutor(
                idleCapacity,
                maxCapacity,
                60,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                threadFactory
        );
        return new ThreadPoolWorkExecutor(messageWorkerPool, new WorkPermitHolder(capacity));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdownNow() {
        pooledExecutor.shutdownNow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void submitWorkOrders(WorkOrderFactory workOrderFactory) throws InterruptedException {
        if (pooledExecutor.isShutdown()) {
            throw new QueueBaccaException("The executor has been shutdown and cannot accept new work orders");
        }

        Queue<Permit> permits = new LinkedList<>(permitHolder.acquireAvailable());
        int availableCapacity = permits.size();

        try {
            Collection<WorkOrder> workOrders = workOrderFactory.create(availableCapacity);
            if (workOrders.size() > availableCapacity) {
                throw new QueueBaccaException("The number of work orders, {0}, cannot exceed the available capacity, {1}", workOrders.size(), availableCapacity);
            }

            workOrders.forEach(workOrder -> submitWorkOrder(workOrder, permits.poll()));
        } finally {
            permits.iterator().forEachRemaining(Permit::release);
        }
    }

    private void submitWorkOrder(WorkOrder workOrder, Permit permit) {
        pooledExecutor.submit(() -> {
            try {
                workOrder.perform();
            } finally {
                permit.release();
            }
        });
    }
}
