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

package io.axonif.queuebacca.exceptions;

import java.text.MessageFormat;

public class QueuebaccaException extends RuntimeException {

    private static final long serialVersionUID = -4021192956335184825L;

    public QueuebaccaException() {
        // Do nothing
    }

    public QueuebaccaException(String message, Object... parameters) {
        super(formatMessage(message, parameters));
    }

    public QueuebaccaException(String message, Throwable cause, Object... parameters) {
        super(formatMessage(message, parameters), cause);
    }

    public QueuebaccaException(Throwable cause) {
        super(cause);
    }

    /**
     * Formats the message with the provided parameters.
     */
    protected static String formatMessage(String message, Object... parameters) {
        return parameters == null || parameters.length == 0 ? message : new MessageFormat(message).format(parameters);
    }
}
