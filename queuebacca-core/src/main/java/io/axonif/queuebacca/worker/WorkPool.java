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

package io.axonif.queuebacca.worker;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.axonif.queuebacca.WorkPermitHolder;
import io.axonif.queuebacca.exceptions.QueuebaccaException;

public final class WorkPool {

    private final Map<License, WorkPermitHolder> licensedPermitHolders = new ConcurrentHashMap<>();
    private final Map<License, Set<Future<?>>> licensedKnownTasks = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final String label;

    private WorkPool(String label) {
        // Create a better language between starting and stopping the pool
        this.label = label;
    }

    public static WorkPool start(String label) {
        return new WorkPool(label);
    }

    public License acquireLicense(String label, int permitLimit) {
        requireNonNull(label);

        License license = new License(label);
        licensedPermitHolders.put(license, new WorkPermitHolder(permitLimit));
        return license;
    }

    public void submitTask(License license, Task task) {
        submitTasks(license, 1, l -> {
            if ()
        });
    }

    public void submitTasks(License license, int requestedLimit, TaskFactory taskFactory) throws InterruptedException {
        requireNonNull(license);
        requireNonNull(taskFactory);

        WorkPermitHolder permitHolder = licensedPermitHolders.get(license);
        Queue<WorkPermitHolder.Permit> permits = new LinkedList<>(permitHolder.acquireAvailable(requestedLimit));
        int availableCapacity = permits.size();

        try {
            Collection<Task> tasks = taskFactory.create(availableCapacity);
            if (tasks.size() > availableCapacity) {
                throw new QueuebaccaException("The number of tasks, {0}, cannot exceed the available capacity, {1}", tasks.size(), availableCapacity);
            }

            Set<Future<?>> knownTasks = licensedKnownTasks.computeIfAbsent(license, l -> ConcurrentHashMap.newKeySet());
            knownTasks.removeIf(Future::isDone);

            tasks.forEach(workOrder -> {
                WorkPermitHolder.Permit permit = permits.remove();
                Future<?> taskFuture = executor.submit(() -> {
                    Thread.currentThread().setName(label + "-" + license.getLabel());
                    try {
                        workOrder.perform();
                    } finally {
                        permit.release();
                        Thread.currentThread().setName(label + "-idle");
                    }
                });
                knownTasks.add(taskFuture);
            });
        } finally {
            permits.iterator().forEachRemaining(WorkPermitHolder.Permit::release);
        }
    }

    public void cancelTasks(License license) {
        requireNonNull(license);

        if (licensedKnownTasks.containsKey(license)) {
            licensedKnownTasks.get(license).forEach(future -> future.cancel(true));
        }
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    public static final class License {

        private final String label;

        private License(String label) {
            this.label = label;
        }

        public String getLabel() {
            return label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            License license = (License) o;
            return label.equals(license.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label);
        }
    }
}
