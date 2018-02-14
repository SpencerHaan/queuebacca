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

package io.axonif.queuebacca.retries;

import io.axonif.queuebacca.RetryDelayGenerator;

/**
 * A {@link RetryDelayGenerator} that returns a constant retry delay regardless of the number of attempts.
 */
public class ConstantRetryDelay implements RetryDelayGenerator {

    private final int retryDelay;

    /**
     * Creates a new instance of a {@link ConstantRetryDelay} with the given retry delay.
     *
     * @param retryDelay the retry delay in seconds
     */
    public ConstantRetryDelay(int retryDelay) {
        this.retryDelay = retryDelay;
    }

    /**
     * Returns the configured, constant retry delay, regardless of the number of attempts.
     *
     * @param attempts the number of attempts
     * @return the retry delay
     */
    @Override
    public int nextRetryDelay(int attempts) {
        return retryDelay;
    }
}
