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

/**
 * A simple courier where the name of the {@link Courier} is applied as a prefix to the processing and recycling
 * {@link MessageBin MessageBins}.
 */
public final class SimpleCourier implements Courier {

    private final String name;
    private final MessageBin processingBin;
    private final MessageBin recyclingBin;

    /**
     * Creates a new instance of a {@link SimpleCourier} with the provided name.
     *
     * @param name the courier name
     */
    public SimpleCourier(String name) {
        this.name = requireNonNull(name);
        this.processingBin = new MessageBin(name + "-processing");
        this.recyclingBin = new MessageBin(name + "-recycling");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageBin getProcessingBin() {
        return processingBin;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageBin getRecyclingBin() {
        return recyclingBin;
    }
}
