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

package dev.haan.queuebacca.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.AmazonSQSException;

import dev.haan.queuebacca.MessageContext;
import dev.haan.queuebacca.ExceptionResolver;
import dev.haan.queuebacca.MessageResponse;

public class AmazonSqsExceptionHandler implements ExceptionResolver.ExceptionHandler<AmazonSQSException> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AmazonSqsExceptionHandler.class);

	@Override
	public MessageResponse handle(AmazonSQSException exception, MessageContext messageContext) {
		LOGGER.error("SQS error occurred", exception);
		return MessageResponse.RETRY;
	}
}
