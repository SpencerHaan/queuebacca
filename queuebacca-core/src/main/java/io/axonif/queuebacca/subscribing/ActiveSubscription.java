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

package io.axonif.queuebacca.subscribing;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.ExceptionResolver;
import io.axonif.queuebacca.IncomingEnvelope;
import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.MessageContext;
import io.axonif.queuebacca.MessageResponse;
import io.axonif.queuebacca.RetryDelayGenerator;
import io.axonif.queuebacca.ThreadPoolWorkExecutor;
import io.axonif.queuebacca.WorkExecutor;
import io.axonif.queuebacca.events.TimingEventSupport;

/**
 * An active subscription that periodically checks for new messages, distributing the consumption using a {@link ThreadPoolWorkExecutor}.
 *
 * @param <M> the message type
 */
class ActiveSubscription<M extends Message> {

    private static final int SUBSCRIPTION_INITIAL_DELAY = 1000; // milliseconds
    private static final int SUBSCRIPTION_PERIODIC_DELAY = 1; // milliseconds

    private final Logger logger;
    private final ScheduledExecutorService subscriptionScheduler;

    private final Client client;
    private final MessageBin messageBin;
    private final WorkExecutor workExecutor;
    private final RetryDelayGenerator retryDelayGenerator;
    private final MessageConsumerSelector<M> consumerSelector;
    private final ExceptionResolver exceptionResolver;
    private final TimingEventSupport timingEventSupport;

    ActiveSubscription(
            ScheduledExecutorService subscriptionScheduler, Client client,
            MessageBin messageBin,
            WorkExecutor workExecutor,
            RetryDelayGenerator retryDelayGenerator,
            MessageConsumerSelector<M> consumerSelector,
            ExceptionResolver exceptionResolver,
            TimingEventSupport timingEventSupport,
            Logger logger
    ) {
        this.subscriptionScheduler = subscriptionScheduler;
        this.client = client;
        this.messageBin = messageBin;
        this.workExecutor = workExecutor;
        this.retryDelayGenerator = retryDelayGenerator;
        this.consumerSelector = consumerSelector;
        this.exceptionResolver = exceptionResolver;
        this.timingEventSupport = timingEventSupport;
        this.logger = logger;
    }

    static <M extends Message> ActiveSubscription<M> start(
            Client client,
            MessageBin messageBin,
            WorkExecutor processingPool,
            MessageConsumerSelector<M> consumerSelector,
            RetryDelayGenerator retryDelayGenerator,
            ExceptionResolver exceptionResolver,
            TimingEventSupport timingEventSupport
    ) {
        ScheduledExecutorService subscriptionScheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                .setNameFormat(messageBin.getName() + "-subscription-%d")
                .build());

        Logger logger = LoggerFactory.getLogger(messageBin.getName());
        ActiveSubscription<M> subscription = new ActiveSubscription<>(
                subscriptionScheduler,
                client,
                messageBin,
                processingPool,
                retryDelayGenerator,
                consumerSelector,
                exceptionResolver,
                timingEventSupport,
                logger
        );

        subscriptionScheduler.scheduleAtFixedRate(() -> {
            try {
                subscription.check();
            } catch (InterruptedException e) {
                // Do nothing
            } catch (Exception e) {
                logger.error("Exception occurred while checking a subscription", e);
            } catch (Error e) {
                logger.error("Error occurred while checking a subscription", e);
                throw e;
            }
        }, SUBSCRIPTION_INITIAL_DELAY, SUBSCRIPTION_PERIODIC_DELAY, TimeUnit.MILLISECONDS);
        return subscription;
    }

    void cancel() {
        logger.info("Cancelling subscription to '{}'", messageBin.getName());
        subscriptionScheduler.shutdownNow();
        workExecutor.shutdownNow();
    }

    private void check() throws InterruptedException {
        workExecutor.submitWorkOrders(capacity -> {
            Collection<IncomingEnvelope<M>> envelopes = client.retrieveMessages(messageBin, capacity);
            return envelopes.stream()
                    .map(this::newWorkOrder)
                    .collect(Collectors.toList());
        });
    }

    private ThreadPoolWorkExecutor.WorkOrder newWorkOrder(IncomingEnvelope<M> envelope) {
        return () -> {
            MDC.put("queuebaccaMessageId", envelope.getMessageId());
            MDC.put("queuebaccaMessageReadCount", String.valueOf(envelope.getReadCount()));

            int currentReadCountThreshold = consumerSelector.currentReadCountThreshold(envelope.getReadCount());
            int currentConsumersReadCount = envelope.getReadCount() - currentReadCountThreshold;
            MessageContext messageContext = new MessageContext(envelope.getMessageId(), currentConsumersReadCount, envelope.getFirstReceived(), envelope.getRawMessage());
            try {
                MessageResponse messageResponse;
                try {
                    long start = System.currentTimeMillis();
                    try {
                        messageResponse = consumerSelector.select(envelope.getReadCount())
                                .consume(envelope.getMessage(), messageContext);
                    } finally {
                        long duration = System.currentTimeMillis() - start;
                        timingEventSupport.fireEvent(messageBin, envelope.getMessage().getClass(), envelope.getMessageId(), duration);
                    }
                } catch (Exception e) {
                    messageResponse = exceptionResolver.resolve(e, messageContext);
                }
                handleResponse(messageResponse, envelope, currentConsumersReadCount);
            } catch (Error e) {
                logger.error("Error occurred while processing message: '{}' with body '{}'", new Object[]{ envelope.getMessageId(), envelope.getRawMessage(), e });
                throw e;
            } finally {
                MDC.clear();
            }
        };
    }

    private void handleResponse(MessageResponse messageResponse, IncomingEnvelope<M> envelope, int readCount) {
        switch (messageResponse) {
            case CONSUMED:
                logger.info("Consumed '{}'; disposing", envelope.getMessageId());
                client.disposeMessage(messageBin, envelope);
                break;
            case RETRY:
                int retryDelay = retryDelayGenerator.nextRetryDelay(readCount);
                logger.warn("Retrying Message '{}' with body {} after {} seconds", new Object[]{ envelope.getMessageId(), envelope.getRawMessage(), retryDelay });
                client.returnMessage(messageBin, envelope, retryDelay);
                break;
            case TERMINATE:
                logger.warn("Terminated '{}' with body {}; disposing", envelope.getMessageId(), envelope.getRawMessage());
                client.disposeMessage(messageBin, envelope);
                break;
            default:
                logger.error("Unknown exception resolution, {}, for '{}'  with body {}; disposing", new Object[]{messageResponse, envelope.getMessageId(), envelope.getRawMessage() });
                client.disposeMessage(messageBin, envelope);
                break;
        }
    }
}
