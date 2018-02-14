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

/**
 * Represents a collection of {@link MessageBin MessageBins} with a root name.
 */
public interface Courier {

    /**
     * Gets the name of the {@link Courier}.
     *
     * @return the courier name.
     */
    String getName();

    /**
     * Gets the {@link MessageBin} representing the processing bin for this {@link Courier}.
     *
     * @return the processing bin
     */
    MessageBin getProcessingBin();

    /**
     * Gets the {@link MessageBin} representing the recycling bin for this {@link Courier}.
     *
     * @return the recycling bin
     */
    MessageBin getRecyclingBin();
}
