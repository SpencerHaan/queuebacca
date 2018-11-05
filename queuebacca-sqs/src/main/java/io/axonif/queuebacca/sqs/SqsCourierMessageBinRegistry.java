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

package io.axonif.queuebacca.sqs;

import static java.util.Objects.requireNonNull;

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

    private static final String SQS_QUEUE_PREFIX = "queuebacca.sqs.queues";
    private static final String SQS_QUEUE_PROCESSING = "processing";
    private static final String SQS_QUEUE_RECYCLING = "recycling";

    public static final String SQS_QUEUE_DISCRIMINATOR = SQS_QUEUE_PREFIX + ".discriminator";
    public static final String SQS_QUEUE_ALLOW_PROVISIONING = SQS_QUEUE_PREFIX + ".allowProvisioning";

    public static final int DEFAULT_RETRIES = 15;
    public static final int DEFAULT_RETRIES_RECYCLING = 15;
    public static final int DEFAULT_DELAY_SECONDS = 0;
    public static final int DEFAULT_MAXIMUM_MESSAGE_SIZE = 262144;
    public static final int DEFAULT_MESSAGE_RETENTION_PERIOD = 345600;
    public static final int DEFAULT_RECEIVE_MESSAGE_WAIT_TIME_SECONDS = 0;
    public static final int DEFAULT_VISIBILITY_TIMEOUT = 600;

    private final Map<String, String> queueUrls = new ConcurrentHashMap<>();
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
        this.trashQueueUrl = registerMessageBin(requireNonNull(trashBin));
        if (allowProvisioning) {
            setQueueAttributes(trashQueueUrl, new BaseConfiguration(), "");
        }
    }

    /**
     * Gets the queue's URL given the {@link MessageBin}.
     *
     * @param messageBin the message bin
     * @return the queue's URL
     */
    public String getQueueUrl(MessageBin messageBin) {
        return queueUrls.get(requireNonNull(messageBin).getName());
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
        String processingQueueUrl = registerMessageBin(processingBin);

        MessageBin recyclingBin = courier.getRecyclingBin();
        String recyclingQueueUrl = registerMessageBin(recyclingBin);

        if (allowProvisioning) {
            SqsRedrivePolicy processingRedrivePolicy = new SqsRedrivePolicy(configuration.getInt("retries", DEFAULT_RETRIES), getQueueArn(recyclingQueueUrl));
            setQueueAttributes(processingQueueUrl, configuration.subset(SQS_QUEUE_PROCESSING), jsonSerializer.toJson(processingRedrivePolicy));
            setTags(processingQueueUrl, courier.getTags());

            SqsRedrivePolicy recyclingRedrivePolicy = new SqsRedrivePolicy(configuration.getInt("retriesRecycling", DEFAULT_RETRIES_RECYCLING), getQueueArn(trashQueueUrl));
            setQueueAttributes(recyclingQueueUrl,  configuration.subset(SQS_QUEUE_RECYCLING), jsonSerializer.toJson(recyclingRedrivePolicy));
            if (!courier.getTags().isEmpty()) {
                setTags(processingQueueUrl, courier.getTags());
                setTags(recyclingQueueUrl, courier.getTags());
            }
            LOGGER.info("Provisioned queues for courier '{}'", courier.getName());
        }
        return this;
    }

    private String registerMessageBin(MessageBin messageBin) {
        String messageBinLabel = messageBin.getName();
        if (queueUrls.containsKey(messageBinLabel)) {
            throw new QueuebaccaConfigurationException("Message bin '" + messageBinLabel + "' has already been registered");
        }

        String queueUrl = createQueue(messageBinLabel);
        queueUrls.put(messageBinLabel, queueUrl);
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

    private void setQueueAttributes(String queueUrl, Configuration configuration, String redrivePolicy) {
        SetQueueAttributesRequest attributesRequest = new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .addAttributesEntry(QueueAttributeName.DelaySeconds.toString(), String.valueOf(configuration.getInt("delaySeconds", DEFAULT_DELAY_SECONDS)))
                .addAttributesEntry(QueueAttributeName.MaximumMessageSize.toString(), String.valueOf(configuration.getInt("maximumMessageSize", DEFAULT_MAXIMUM_MESSAGE_SIZE)))
                .addAttributesEntry(QueueAttributeName.MessageRetentionPeriod.toString(), String.valueOf(configuration.getInt("messageRetentionPeriod", DEFAULT_MESSAGE_RETENTION_PERIOD)))
                .addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(), String.valueOf(configuration.getInt("receiveMessageWaitTimeSeconds", DEFAULT_RECEIVE_MESSAGE_WAIT_TIME_SECONDS)))
                .addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), String.valueOf(configuration.getInt("visibilityTimeout", DEFAULT_VISIBILITY_TIMEOUT)))
                .addAttributesEntry(QueueAttributeName.RedrivePolicy.toString(), redrivePolicy);

        client.setQueueAttributes(attributesRequest);
    }

    private void setTags(String queueUrl, Collection<SqsTag> tags) {
        Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(SqsTag::getKey, SqsTag::getValue));
        TagQueueRequest tagQueueRequest = new TagQueueRequest(queueUrl, tagMap);

        client.tagQueue(tagQueueRequest);
    }
}
