/*
 * Copyright 2018 The QueueBacca Authors
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

import static io.axonif.queuebacca.ExceptionResolver.Resolution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.AmazonSQSException;
import io.axonif.queuebacca.Context;
import io.axonif.queuebacca.ExceptionResolver;

public class AmazonSqsExceptionHandler implements ExceptionResolver.ExceptionHandler<AmazonSQSException> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AmazonSqsExceptionHandler.class);

	@Override
	public Resolution handle(AmazonSQSException exception, Context context) {
		LOGGER.error("SQS error occurred", exception);
		return Resolution.RETRY;
	}
}
