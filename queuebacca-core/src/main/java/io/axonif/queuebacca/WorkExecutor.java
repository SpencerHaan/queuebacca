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

import java.util.Collection;

public interface WorkExecutor {

	/**
	 * Attempts to stop all currently executing workers and prevents any future workers from being executed.
	 */
	void shutdownNow();

	/**
	 * Submits {@link WorkOrder WorkOrders} using the provided {@link WorkOrderFactory}. The executor should block if it
	 * is unable to submit new work orders immediately.
	 *
	 * @param workOrderFactory the factory to produce the work orders
	 * @throws InterruptedException if the executor is interrupted while waiting to submit
	 */
	void submitWorkOrders(WorkOrderFactory workOrderFactory) throws InterruptedException;

	/**
	 * A functional interface representing a unit of work as a work order.
	 */
	@FunctionalInterface
	interface WorkOrder {

		/**
		 * Perform the unit of work.
		 */
		void perform();
	}

	/**
	 * A factory for creating {@link WorkOrder WorkOrders}.
	 */
	@FunctionalInterface
	interface WorkOrderFactory {

		/**
		 * Creates {@link WorkOrder WorkOrders} up to the given maximum.
		 *
		 * @param maximum the maximum work orders that can be created
		 * @return zero or more work orders, up to the capacity
		 */
		Collection<WorkOrder> create(int maximum) throws InterruptedException;
	}
}
