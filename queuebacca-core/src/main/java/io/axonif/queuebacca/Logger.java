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

import io.axonif.queuebacca.util.MessageSerializer;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class Logger {

	public interface MessageMarker {

		Object get();
	}

	private final org.slf4j.Logger logger;
	private final MessageSerializer messageSerializer;

	Logger(String name, MessageSerializer messageSerializer) {
		this.logger = LoggerFactory.getLogger(name);
		this.messageSerializer = requireNonNull(messageSerializer);
	}

	public static MessageMarker markMessage(Object message) {
		return () -> message;
	}

	public void info(String format, Object... arguments) {
		log(format, arguments, logger::warn);
	}

	public void warn(String format, Object... arguments) {
		log(format, arguments, logger::warn);
	}

	public void error(String format, Object... arguments) {
		log(format, arguments, logger::error);
	}

	private void log(String format, Object[] arguments, BiConsumer<String, Object[]> method) {
		requireNonNull(format);
		requireNonNull(arguments);

		method.accept(format, massageArguments(arguments));
	}

	private Object[] massageArguments(Object[] arguments) {
		return Stream.of(arguments)
				.map(this::massageArgument)
				.toArray(Object[]::new);
	}

	private Object massageArgument(Object argument) {
		if (argument instanceof MessageMarker) {
			return messageSerializer.toString(((MessageMarker) argument).get());
		} else {
			return argument;
		}
	}
}
