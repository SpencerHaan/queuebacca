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

package io.axonif.queuebacca;

//import static org.junit.Assert.assertEquals;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import io.axonif.via.sqs.JacksonSerializer;
//import org.junit.Test;
//
//import io.axonif.via.retries.ConstantRetryDelay;
//import io.axonif.via.sqs.JsonMessageSerializer;
//import io.axonif.via.util.MessageSerializer;
//import com.fasterxml.jackson.databind.ObjectMapper;

public class ViaIntegrationTest {

//	private ViaExceptionResolver exceptionResolver = ViaExceptionResolver.builder().build();
//	private MessageSerializer serializer = new JsonMessageSerializer(new JacksonSerializer(new ObjectMapper()));
//
//	@Test
//	public void subscribe() throws InterruptedException {
//		ViaMessageBin messageBin = new ViaMessageBin("test");
//		TestViaClient viaClient = new TestViaClient();
//		ViaSubscriber subscriber = new ViaSubscriber(viaClient, ThreadPoolWorkExecutor::newPooledWorkExecutor, exceptionResolver, serializer);
//
//		ViaMessageConsumer<TestMessage> consumer = (message, context) -> message.markComplete();
//		ViaSubscriptionConfiguration<TestMessage> configuration = ViaSubscriptionConfiguration.builder(messageBin, consumer)
//				.withMessageCapacity(2)
//				.build();
//		subscriber.subscribe(configuration);
//
//		ViaPublisher publisher = new ViaPublisher(viaClient, messageBin);
//
//		CountDownLatch countDownLatch = new CountDownLatch(10);
//		Collection<TestMessage> messages = new ArrayList<>();
//		for (int i = 0; i < 10; i++) {
//			messages.add(new TestMessage(countDownLatch::countDown));
//		}
//		publisher.publish(messages);
//
//		// If a message doesn't finish, this will timeout
//		countDownLatch.await(10, TimeUnit.SECONDS);
//
//		subscriber.cancelAll();
//	}
//
//	@Test
//	public void subscribe_FailedMessage() throws InterruptedException {
//		ViaMessageBin messageBin = new ViaMessageBin("test");
//		TestViaClient viaClient = new TestViaClient();
//		ViaSubscriber subscriber = new ViaSubscriber(viaClient, ThreadPoolWorkExecutor::newPooledWorkExecutor, exceptionResolver, serializer);
//
//		ViaMessageConsumer<TestMessage> consumer = (message, context) -> message.markComplete();
//		ViaSubscriptionConfiguration<TestMessage> configuration = ViaSubscriptionConfiguration.builder(messageBin, consumer)
//				.withMessageCapacity(2)
//				.withRetryDelayGenerator(new ConstantRetryDelay(0))
//				.build();
//		subscriber.subscribe(configuration);
//
//		ViaPublisher publisher = new ViaPublisher(viaClient, messageBin);
//
//		CountDownLatch countDownLatch = new CountDownLatch(10);
//		Map<TestMessage, AtomicInteger> messageCounters = new ConcurrentHashMap<>();
//		for (int i = 0; i < 10; i++) {
//			AtomicInteger counter = new AtomicInteger(0);
//			Runnable runnable = () -> {
//				if (counter.getAndIncrement() == 0) {
//					throw new RuntimeException("Counter is 0, oh no!");
//				}
//				countDownLatch.countDown();
//			};
//			messageCounters.put(new TestMessage(runnable), counter);
//		}
//		publisher.publish(messageCounters.keySet());
//
//		// If a message doesn't finish, this will timeout
//		countDownLatch.await(10, TimeUnit.SECONDS);
//
//		subscriber.cancelAll();
//
//		for(AtomicInteger counter : messageCounters.values()) {
//			assertEquals(2, counter.get());
//		}
//	}
//
//	private class TestMessage implements ViaMessage {
//
//		private final Runnable onComplete;
//
//		private TestMessage(Runnable onComplete) {
//			this.onComplete = onComplete;
//		}
//
//		public void markComplete() {
//			onComplete.run();
//		}
//	}
}