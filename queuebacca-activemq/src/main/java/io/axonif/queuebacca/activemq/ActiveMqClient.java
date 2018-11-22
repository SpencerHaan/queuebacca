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

package io.axonif.queuebacca.activemq;

import java.util.Collection;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQTextMessage;

import io.axonif.queuebacca.Client;
import io.axonif.queuebacca.IncomingEnvelope;
import io.axonif.queuebacca.Message;
import io.axonif.queuebacca.MessageBin;
import io.axonif.queuebacca.OutgoingEnvelope;

public class ActiveMqClient implements Client {

    private static final int LONG_POLL_TIMEOUT_MILLISECONDS = 20000;

    private final ActiveMQConnectionFactory factory;

    @Override
    public <M extends Message> OutgoingEnvelope<M> sendMessage(MessageBin messageBin, M message, int delay) {
        return null;
    }

    @Override
    public <M extends Message> Collection<OutgoingEnvelope<M>> sendMessages(MessageBin messageBin, Collection<M> messages, int delay) {
        try {
            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);
            session.createTextMessage();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <M extends Message> Collection<IncomingEnvelope<M>> retrieveMessages(MessageBin messageBin, int maxMessages) {
        try {
            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(null, "");

            do {
                javax.jms.Message message = consumer.receive(LONG_POLL_TIMEOUT_MILLISECONDS);
                message.
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void returnMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope, int delay) {
    }

    @Override
    public void disposeMessage(MessageBin messageBin, IncomingEnvelope<?> incomingEnvelope) {

    }
}
