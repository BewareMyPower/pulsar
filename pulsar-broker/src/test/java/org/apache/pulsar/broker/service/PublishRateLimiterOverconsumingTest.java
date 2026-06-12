/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class PublishRateLimiterOverconsumingTest extends BrokerTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * This test verifies the broker publish rate limiting behavior with multiple concurrent publishers.
     * This reproduces the issue https://github.com/apache/pulsar/issues/23920 and prevents future regressions.
     */
    @Test
    public void testOverconsumingTokensWithBrokerPublishRateLimiter() throws Exception {
        int rateInMsg = 500;
        int durationSeconds = 5;
        int numberOfProducersWithIndependentClients = 5;
        int numberOfMessagesForEachProducer = (rateInMsg * (durationSeconds + 1))
                / numberOfProducersWithIndependentClients;
        int totalMessages = numberOfProducersWithIndependentClients * numberOfMessagesForEachProducer;
        List<Long> sendTimestamps = Collections.synchronizedList(new ArrayList<>());

        // configure publish throttling rate
        BrokerService brokerService = pulsar.getBrokerService();
        admin.brokers().updateDynamicConfiguration("brokerPublisherThrottlingMaxMessageRate",
                String.valueOf(rateInMsg));
        Awaitility.await().untilAsserted(() -> {
            PublishRateLimiterImpl publishRateLimiter =
                    (PublishRateLimiterImpl) brokerService.getBrokerPublishRateLimiter();
            AsyncTokenBucket tokenBucketOnMessage = publishRateLimiter.getTokenBucketOnMessage();
            assertThat(tokenBucketOnMessage).isNotNull();
            assertEquals(tokenBucketOnMessage.getRate(), rateInMsg);
            assertNull(publishRateLimiter.getTokenBucketOnByte());
        });
        AsyncTokenBucket tokenBucketOnMessage =
                ((PublishRateLimiterImpl) brokerService.getBrokerPublishRateLimiter()).getTokenBucketOnMessage();

        final String topicName = "persistent://" + newTopicName();

        // create independent clients so each producer exercises throttling on its own connection
        @SuppressWarnings("deprecation")
        List<PulsarClient> producerClients = IntStream.range(0, numberOfProducersWithIndependentClients)
                .mapToObj(i -> {
                    try {
                        return PulsarClient.builder()
                                .serviceUrl(pulsar.getBrokerServiceUrl())
                                .ioThreads(1)
                                .statsInterval(0, TimeUnit.SECONDS)
                                .connectionsPerBroker(1)
                                .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toList();
        @Cleanup
        AutoCloseable producerClientsCloser = () -> {
            producerClients.forEach(c -> {
                try {
                    c.close();
                } catch (Exception e) {
                    // ignore
                }
            });
        };

        List<Producer<Integer>> producers = IntStream.range(0, numberOfProducersWithIndependentClients)
                .mapToObj(i -> {
                    try {
                        return producerClients.get(i)
                                .newProducer(Schema.INT32).enableBatching(false)
                                .producerName("producer-" + (i + 1))
                                .topic(topicName).create();
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                }).toList();

        @Cleanup
        AutoCloseable producersClose = () -> {
            producers.forEach(p -> {
                try {
                    p.close();
                } catch (Exception e) {
                    // ignore
                }
            });
        };

        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(numberOfProducersWithIndependentClients);
        CountDownLatch ready = new CountDownLatch(numberOfProducersWithIndependentClients);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Void>> sendTasks = IntStream.range(0, numberOfProducersWithIndependentClients)
                .mapToObj(i -> {
                    Callable<Void> task = () -> {
                        ready.countDown();
                        start.await();
                        Producer<Integer> producer = producers.get(i);
                        for (int messageNumber = 0; messageNumber < numberOfMessagesForEachProducer; messageNumber++) {
                            producer.send(messageNumber);
                            sendTimestamps.add(System.nanoTime());
                        }
                        return null;
                    };
                    return executor.submit(task);
                })
                .toList();

        assertThat(ready.await(10, TimeUnit.SECONDS))
                .describedAs("all producer tasks are ready to send")
                .isTrue();

        long sendStartNanos = System.nanoTime();
        start.countDown();
        for (Future<?> sendTask : sendTasks) {
            sendTask.get(durationSeconds * 4L, TimeUnit.SECONDS);
        }
        long sendDurationNanos = System.nanoTime() - sendStartNanos;

        long messagesAfterInitialBucket = Math.max(0, totalMessages - tokenBucketOnMessage.getCapacity());
        double refillRate = messagesAfterInitialBucket / (sendDurationNanos / 1_000_000_000.0d);
        log.info().attr("totalMessages", totalMessages)
                .attr("sendDurationMillis", TimeUnit.NANOSECONDS.toMillis(sendDurationNanos))
                .attr("initialBucketCapacity", tokenBucketOnMessage.getCapacity())
                .attr("refillRate", refillRate)
                .log("Published messages under broker publish rate limiter");
        assertThat(refillRate)
                .describedAs("publish rate after the initial token bucket capacity is consumed")
                .isLessThanOrEqualTo(rateInMsg * 1.4d);

        // Per-second window check to catch intermittent overconsumption that could be hidden in the average
        List<Long> sortedTimestamps = new ArrayList<>(sendTimestamps);
        Collections.sort(sortedTimestamps);
        List<Long> perSecondCounts = new ArrayList<>();
        int idx = 0;
        for (int window = 0; idx < sortedTimestamps.size(); window++) {
            long windowEnd = sendStartNanos + (window + 1) * 1_000_000_000L;
            int count = 0;
            while (idx < sortedTimestamps.size() && sortedTimestamps.get(idx) < windowEnd) {
                count++;
                idx++;
            }
            perSecondCounts.add((long) count);
        }
        // Skip first window (initial burst) and last window (may be partial).
        // Use 2-second sliding window averages to smooth out GC pauses and scheduling variance.
        for (int i = 1; i < perSecondCounts.size() - 2; i++) {
            long rateTwoSecond = perSecondCounts.get(i) + perSecondCounts.get(i + 1);
            log.info().attr("windowStartSecond", i).attr("rateTwoSecond", rateTwoSecond)
                    .log("2-second sliding window publish rate");
            assertThat((double) rateTwoSecond)
                    .describedAs("2-second sliding window publish rate starting at second %d", i)
                    .isLessThanOrEqualTo(rateInMsg * 2 * 1.55d);
        }
    }
}
