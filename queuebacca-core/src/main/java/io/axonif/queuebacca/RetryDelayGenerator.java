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

package io.axonif.queuebacca;

/**
 * Generates a retry delay based on the number of attempts at consuming a message.
 */
public interface RetryDelayGenerator {

    /**
     * Gets the next retry delay based on the number of attempts provided.
     *
     * @param attempts the number of attempts
     * @return the next retry delay
     */
    int nextRetryDelay(int attempts);
}
