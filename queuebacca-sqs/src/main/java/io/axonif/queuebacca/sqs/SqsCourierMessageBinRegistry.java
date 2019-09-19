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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDeletedRecentlyException;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.TagQueueRequest;

import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.exceptions.QueuebaccaConfigurationException;

/**
 * A registry for mapping {@link MessageBin MessageBins} to their corresponding SQS queues. Will also provision
 * the queues (update settings) based on provided configuration, and if allowed.
 */
public final class SqsCourierMessageBinRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqsCourierMessageBinRegistry.class);

    private static final String SQS_TAGGING_RETRY_ATTEMPTS_PROPERTY = "queuebacca.sqs.tagging.retryAttempts";
    private static final String SQS_TAGGING_RETRY_FREQUENCY_PROPERTY = "queuebacca.sqs.tagging.retryFrequency";

    private static final int SQS_TAGGING_RETRY_ATTEMPTS_DEFAULT = 5;
    private static final int SQS_TAGGING_RETRY_FREQUENCY_DEFAULT = 1000;

    private static final String SQS_QUEUE_PREFIX = "queuebacca.sqs.queues";
    private static final String SQS_QUEUE_PROCESSING = "processing";
    private static final String SQS_QUEUE_RECYCLING = "recycling";

    private static final int DEFAULT_RETRIES = 15;
    private static final int DEFAULT_RETRIES_RECYCLING = 15;
    private static final int DEFAULT_DELAY_SECONDS = 0;
    private static final int DEFAULT_MAXIMUM_MESSAGE_SIZE = 262144;
    private static final int DEFAULT_MESSAGE_RETENTION_PERIOD = 345600;
    private static final int DEFAULT_RECEIVE_MESSAGE_WAIT_TIME_SECONDS = 0;
    private static final Duration TRASH_QUEUE_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    private final Map<String, CourierProfile> courierProfiles = new ConcurrentHashMap<>();
    private final AmazonSQS client;
    private final JsonSerializer jsonSerializer;
    private final String queueNameDiscriminator;
    private final boolean allowProvisioning;

    private final String trashQueueUrl;

    /**
     * Creates a new instance of a {@link SqsCourierMessageBinRegistry}.
     *
     * @param client the AWS SQS client
     * @param jsonSerializer a serializer for turning objects into JSON strings
     * @param queueNameDiscriminator a discriminator that is prefixed to queues
     * @param allowProvisioning if this registry is allowed to provision queues
     * @param trashBin a trash bin to act as a recycling bins dead letter queue
     */
    public SqsCourierMessageBinRegistry(AmazonSQS client, JsonSerializer jsonSerializer, String queueNameDiscriminator, boolean allowProvisioning, MessageBin trashBin) {
        this.client = requireNonNull(client);
        this.jsonSerializer = requireNonNull(jsonSerializer);
        this.queueNameDiscriminator = requireNonNull(queueNameDiscriminator);
        this.allowProvisioning = allowProvisioning;
        this.trashQueueUrl = registerMessageBin(requireNonNull(trashBin), TRASH_QUEUE_VISIBILITY_TIMEOUT);
        if (allowProvisioning) {
            setQueueAttributes(trashQueueUrl, TRASH_QUEUE_VISIBILITY_TIMEOUT, new BaseConfiguration(), "");
        }
    }

    /**
     * Gets the queue's URL given the {@link MessageBin}.
     *
     * @param messageBin the message bin
     * @return the queue's URL
     */
    public String getQueueUrl(MessageBin messageBin) {
        requireNonNull(messageBin);

        return courierProfiles.get(messageBin.getName()).queueUrl;
    }

    public Duration getVisibilityTimeout(MessageBin messageBin) {
        requireNonNull(messageBin);

        return courierProfiles.get(messageBin.getName()).visibilityTimeout;
    }

    /**
     * Registers a {@link SqsCourier Courier's} processing and recycling {@link MessageBin ViaMessageBins}.
     *
     * @param courier the courier containing the processing and recycling bins
     * @param configuration configuration for the courier's message bins
     * @return this object for method chaining
     */
    public SqsCourierMessageBinRegistry register(SqsCourier courier, Configuration configuration) {
        requireNonNull(courier);
        requireNonNull(configuration);

        configuration = configuration.subset(SQS_QUEUE_PREFIX).subset(courier.getName());

        MessageBin processingBin = courier.getProcessingBin();
        String processingQueueUrl = registerMessageBin(processingBin, courier.getVisibilityTimeout());

        MessageBin recyclingBin = courier.getRecyclingBin();
        String recyclingQueueUrl = registerMessageBin(recyclingBin, courier.getVisibilityTimeout());

        if (allowProvisioning) {
            SqsRedrivePolicy processingRedrivePolicy = new SqsRedrivePolicy(configuration.getInt("retries", DEFAULT_RETRIES), getQueueArn(recyclingQueueUrl));
            setQueueAttributes(processingQueueUrl, courier.getVisibilityTimeout(), configuration.subset(SQS_QUEUE_PROCESSING), jsonSerializer.toJson(processingRedrivePolicy));

            SqsRedrivePolicy recyclingRedrivePolicy = new SqsRedrivePolicy(configuration.getInt("retriesRecycling", DEFAULT_RETRIES_RECYCLING), getQueueArn(trashQueueUrl));
            setQueueAttributes(recyclingQueueUrl,  courier.getVisibilityTimeout(), configuration.subset(SQS_QUEUE_RECYCLING), jsonSerializer.toJson(recyclingRedrivePolicy));

            if (!courier.getTags().isEmpty()) {
                setTags(processingQueueUrl, courier.getTags(), configuration);
                setTags(recyclingQueueUrl, courier.getTags(), configuration);
            }
            LOGGER.info("Provisioned queues for courier '{}'", courier.getName());
        }
        return this;
    }

    private String registerMessageBin(MessageBin messageBin, Duration visibilityTimeout) {
        String messageBinLabel = messageBin.getName();
        if (courierProfiles.containsKey(messageBinLabel)) {
            throw new QueuebaccaConfigurationException("Message bin '" + messageBinLabel + "' has already been registered");
        }

        String queueUrl = createQueue(messageBinLabel);
        courierProfiles.put(messageBinLabel, new CourierProfile(queueUrl, visibilityTimeout));
        return queueUrl;
    }

    private String createQueue(String messageBinLabel) {
        String queueName = (queueNameDiscriminator != null ? queueNameDiscriminator + "-" : "") + messageBinLabel;
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName(queueName);

        try {
            LOGGER.info("Creating queue '{}'", queueName);
            return client.createQueue(request).getQueueUrl();
        } catch (QueueDeletedRecentlyException e) {
            LOGGER.warn("Queue '{}' was recently deleted; waiting 60 seconds before attempting to create", queueName);
            try {
                Thread.sleep(60000);
            } catch (InterruptedException ie) {
                // Do nothing
            }
            return client.createQueue(request).getQueueUrl();
        } catch (QueueNameExistsException e) {
            return client.getQueueUrl(queueName).getQueueUrl();
        } catch (AmazonServiceException e) {
            LOGGER.error("This should not be thrown for a queue that already exists!");
            return client.getQueueUrl(queueName).getQueueUrl();
        }
    }

    private String getQueueArn(String queueUrl) {
        GetQueueAttributesRequest attributesRequest = new GetQueueAttributesRequest(queueUrl)
                .withAttributeNames(QueueAttributeName.QueueArn);
        return client.getQueueAttributes(attributesRequest).getAttributes().get(QueueAttributeName.QueueArn.toString());
    }

    private void setQueueAttributes(String queueUrl, Duration visibilityTimeout, Configuration configuration, String redrivePolicy) {
        SetQueueAttributesRequest attributesRequest = new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .addAttributesEntry(QueueAttributeName.DelaySeconds.toString(), String.valueOf(configuration.getInt("delaySeconds", DEFAULT_DELAY_SECONDS)))
                .addAttributesEntry(QueueAttributeName.MaximumMessageSize.toString(), String.valueOf(configuration.getInt("maximumMessageSize", DEFAULT_MAXIMUM_MESSAGE_SIZE)))
                .addAttributesEntry(QueueAttributeName.MessageRetentionPeriod.toString(), String.valueOf(configuration.getInt("messageRetentionPeriod", DEFAULT_MESSAGE_RETENTION_PERIOD)))
                .addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(), String.valueOf(configuration.getInt("receiveMessageWaitTimeSeconds", DEFAULT_RECEIVE_MESSAGE_WAIT_TIME_SECONDS)))
                .addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), String.valueOf(configuration.getLong("visibilityTimeout", visibilityTimeout.getSeconds())))
                .addAttributesEntry(QueueAttributeName.RedrivePolicy.toString(), redrivePolicy);

        client.setQueueAttributes(attributesRequest);
    }

    private void setTags(String queueUrl, Collection<SqsTag> tags, Configuration configuration) {
        Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(SqsTag::getKey, SqsTag::getValue));
        TagQueueRequest tagQueueRequest = new TagQueueRequest(queueUrl, tagMap);

        int maxAttempts = configuration.getInt(SQS_TAGGING_RETRY_ATTEMPTS_PROPERTY, SQS_TAGGING_RETRY_ATTEMPTS_DEFAULT);
        int currentAttempt = 0;
        do {
            try {
                client.tagQueue(tagQueueRequest);
                break;
            } catch (AmazonSQSException e) {
                if (e.getErrorCode().equals("RequestThrottled")) {
                    try {
                        Thread.sleep(configuration.getInt(SQS_TAGGING_RETRY_FREQUENCY_PROPERTY, SQS_TAGGING_RETRY_FREQUENCY_DEFAULT));
                    } catch (InterruptedException ex) {
                        // Do nothing
                    }
                } else {
                    throw e;
                }
            }
        } while (++currentAttempt < maxAttempts);

        if (currentAttempt == maxAttempts) {
            LOGGER.warn("Failed to tag queue '{}'", queueUrl);
        }
    }

    private class CourierProfile {

        private final String queueUrl;
        private final Duration visibilityTimeout;

        private CourierProfile(String queueUrl, Duration visibilityTimeout) {
            this.queueUrl = requireNonNull(queueUrl);
            this.visibilityTimeout = requireNonNull(visibilityTimeout);
        }
    }
}
