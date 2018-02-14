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
import com.google.common.math.IntMath;

/**
 * A {@link RetryDelayGenerator} that computes an exponential backoff. The backoff starts only after the configured
 * staggered retry attempts, and will return 1 before that.
 */
public class StaggeredExponentialBackoff implements RetryDelayGenerator {

    public static final int DEFAULT_BASE = 2;

    private final int base;
    private final int staggeredAttempts;

    /**
     * Creates a new instance of a {@link StaggeredExponentialBackoff} with the given staggered attempts. The default
     * exponential base is {@link #DEFAULT_BASE}.
     *
     * @param staggeredAttempts number of attempts before backoff
     */
    public StaggeredExponentialBackoff(int staggeredAttempts) {
        this(DEFAULT_BASE, staggeredAttempts);
    }

    /**
     * Creates a new instance of a {@link StaggeredExponentialBackoff} with the given exponential base and staggered attempts.
     *
     * @param base the exponential base
     * @param staggeredAttempts number of attempts before backoff
     */
    public StaggeredExponentialBackoff(int base, int staggeredAttempts) {
        this.base = base;
        this.staggeredAttempts = staggeredAttempts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int nextRetryDelay(int attempts) {
        int power = attempts - staggeredAttempts;
        return attempts > staggeredAttempts
                ? IntMath.pow(base, power) - 1
                : 1;
    }
}
