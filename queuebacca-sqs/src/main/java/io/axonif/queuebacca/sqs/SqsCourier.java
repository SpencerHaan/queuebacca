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

package io.axonif.queuebacca.sqs;

import java.time.Duration;
import java.util.Collection;

import io.axonif.queuebacca.MessageBin;

/**
 * Represents a collection of {@link MessageBin MessageBins} with a root name.
 */
public interface SqsCourier {

    /**
     * Gets the name of the {@link SqsCourier}.
     *
     * @return the courier name.
     */
    String getName();

    /**
     * Gets the {@link MessageBin} representing the processing bin for this {@link SqsCourier}.
     *
     * @return the processing bin
     */
    MessageBin getProcessingBin();

    /**
     * Gets the {@link MessageBin} representing the recycling bin for this {@link SqsCourier}.
     *
     * @return the recycling bin
     */
    MessageBin getRecyclingBin();

    /**
     * Gets a collection of {@link SqsTag SqsTags} related to this {@link SqsCourier}.
     *
     * @return the SQS tags
     */
    Collection<SqsTag> getTags();

    /**
     * Gets the visibility timeout for both the processing and recycling bins.
     *
     * @return the visibility timeout as a {@link Duration}
     */
    Duration getVisibilityTimeout();
}
