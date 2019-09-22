/*
 * Copyright 2019 The Queuebacca Authors
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

import static com.google.common.collect.Iterables.partition;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import io.axonif.queuebacca.OutgoingEnvelope;
import io.axonif.queuebacca.MessageSerializer;

/**
 * Simplifies the batching of messages for SQS, ensuring batch sizes are not exceeded due to large individual
 * {@link M messages}.
 */
final class SqsMessageBatchSender {

    public static final int MAX_BATCH_ENTRY_COUNT = 10;

    private final Collection<BatchEntry> batchEntries = new ArrayList<>();

    private final AmazonSQS client;
    private final Logger logger;

    /**
     * Creates a new instance of a {@link SqsMessageBatchSender} with the given {@link AmazonSQS client} and {@link MessageSerializer}.
     *
     * @param client the AWS SQS client
     */
    SqsMessageBatchSender(AmazonSQS client, Logger logger) {
        this.client = requireNonNull(client);
        this.logger = requireNonNull(logger);
    }

    /**
     * Adds a {@link M message} to be placed into a batch.
     *
     * @param message the message
     */
    void add(String message) {
        requireNonNull(message);

        BatchEntry entry = new BatchEntry(message, message);
        batchEntries.add(entry);
    }

    /**
     * Sends all constructed batches to the given queue URL with the given delay.
     *
     * @param queueUrl the SQS queue URL
     * @param delay a delay of 0 or greater
     * @return a collection of {@link OutgoingEnvelope OutgoingEnvelopes} containing information of the sent messages
     */
    Collection<OutgoingEnvelope> send(String queueUrl, int delay) {
        return StreamSupport.stream(partition(batchEntries, MAX_BATCH_ENTRY_COUNT).spliterator(), false)
                .map(Batch::from)
                .map(b -> sendBatch(b, queueUrl, delay))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Collection<OutgoingEnvelope> sendBatch(Batch batch, String queueUrl, int delay) {
        SendMessageBatchRequest batchRequest = batch.toBatchRequest(delay)
                .withQueueUrl(queueUrl);
        SendMessageBatchResult batchResult = client.sendMessageBatch(batchRequest);

        Collection<OutgoingEnvelope> envelopes = batchResult.getSuccessful().stream()
                .peek(s ->  logger.info("Sent SQS message '{}'", s.getMessageId()))
                .map(s -> new OutgoingEnvelope(s.getMessageId(), batch.getMessage(s.getId())))
                .collect(Collectors.toList());

        batchResult.getFailed().forEach(f -> {
            logger.warn("Batch entry '{}' failed: [{}] {}; retrying", new Object[]{ f.getId(), f.getCode(), f.getMessage() });
            batchRequest.getEntries().stream()
                    .filter(e -> Objects.equals(e.getId(),f.getId()))
                    .findFirst()
                    .ifPresent(e -> {
                        SendMessageRequest retryMessageRequest = new SendMessageRequest(queueUrl, e.getMessageBody());
                        SendMessageResult result = client.sendMessage(retryMessageRequest);
                        logger.info("Sent SQS message '{}'", result.getMessageId());
                        OutgoingEnvelope envelope = new OutgoingEnvelope(result.getMessageId(), batch.getMessage(e.getId()));
                        envelopes.add(envelope);
                    });
        });
        return envelopes;
    }

    /**
     * Represents a single batch, containing up to 10 {@link BatchEntry BatchEntrys}.
     */
    private static class Batch {

        private final Map<String, BatchEntry> entries;

        private Batch(Map<String, BatchEntry> entries) {
            this.entries = requireNonNull(entries);
        }

        public static Batch from(Collection<BatchEntry> entries) {
            String id = UUID.randomUUID().toString().replace("-", "");

            AtomicInteger idCounter = new AtomicInteger(1);
            Map<String, BatchEntry> mappedEntries = entries.stream()
                    .collect(Collectors.toMap(e -> id + "_" + idCounter.getAndIncrement(), Function.identity()));
            return new Batch(mappedEntries);
        }

        private String getMessage(String id) {
            return entries.get(id).getMessage();
        }

        private String getMessageBody(String id) {
            return entries.get(id).getMessageBody();
        }

        private SendMessageBatchRequest toBatchRequest(int delay) {
            Collection<SendMessageBatchRequestEntry> batchRequestEntries = entries.entrySet().stream()
                    .map(es -> new SendMessageBatchRequestEntry(es.getKey(), es.getValue().getMessageBody())
                            .withDelaySeconds(delay)
                    )
                    .collect(Collectors.toList());
            return new SendMessageBatchRequest().withEntries(batchRequestEntries);
        }
    }

    /**
     * A single entry in a {@link Batch} to store the {@link M message} and it's serialized form.
     */
    private static class BatchEntry {

        private final String message;
        private final String messageBody;

        private BatchEntry(String message, String messageBody) {
            this.message = message;
            this.messageBody = messageBody;
        }

        private String getMessage() {
            return message;
        }

        private String getMessageBody() {
            return messageBody;
        }
    }
}
