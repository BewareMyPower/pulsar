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
package org.apache.pulsar.broker.stats;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.base.Splitter;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prometheus.client.Collector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetricsToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.manager.UnloadManager;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.compaction.PulsarCompactionServiceFactory;
import org.apache.zookeeper.CreateMode;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PrometheusMetricsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        AuthenticationMetricsToken.reset();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPublishRateLimitedTimes() throws Exception {
        cleanup();
        conf.setMaxPublishRatePerTopicInMessages(1);
        conf.setMaxPublishRatePerTopicInBytes(1);
        conf.setBrokerPublisherThrottlingMaxMessageRate(100000);
        conf.setBrokerPublisherThrottlingMaxByteRate(10000000);
        conf.setStatsUpdateFrequencyInSecs(100000000);
        setup();
        String ns1 = "prop/ns-abc1" + UUID.randomUUID();
        admin.namespaces().createNamespace(ns1, 1);
        String topicName = "persistent://" + ns1 + "/metrics" + UUID.randomUUID();
        String topicName2 = "persistent://" + ns1 + "/metrics" + UUID.randomUUID();
        String topicName3 = "persistent://" + ns1 + "/metrics" + UUID.randomUUID();
        // Use another connection
        @Cleanup
        PulsarClient client2 = newPulsarClient(lookupUrl.toString(), 0);

        Producer<byte[]> producer = pulsarClient.newProducer().producerName("my-pub").enableBatching(false)
                .topic(topicName).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().producerName("my-pub-2").enableBatching(false)
                .topic(topicName2).create();
        Producer<byte[]> producer3 = client2.newProducer().producerName("my-pub-2").enableBatching(false)
                .topic(topicName3).create();
        producer.sendAsync(new byte[11]);

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).get().get();
        Field field = AbstractTopic.class.getDeclaredField("publishRateLimitedTimes");
        field.setAccessible(true);
        Awaitility.await().untilAsserted(() -> {
            long value = (long) field.get(persistentTopic);
            assertEquals(value, 1);
        });
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        assertTrue(metrics.containsKey("pulsar_publish_rate_limit_times"));
        metrics.get("pulsar_publish_rate_limit_times").forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                if (item.tags.get("topic").equals(topicName)) {
                    assertEquals(item.value, 1);
                    return;
                } else if (item.tags.get("topic").equals(topicName2)) {
                    assertEquals(item.value, 1);
                    return;
                } else if (item.tags.get("topic").equals(topicName3)) {
                    // We only trigger the rate limiting of the topic, so if the topic is not using
                    // the same connection, the rate limiting times will be 0
                    assertEquals(item.value, 0);
                    return;
                }
                fail("should not fail");
            }
        });
        // Stats updater will reset the stats
        pulsar.getBrokerService().updateRates();
        Awaitility.await().untilAsserted(() -> {
            long value = (long) field.get(persistentTopic);
            assertEquals(value, 0);
        });

        @Cleanup
        ByteArrayOutputStream statsOut2 = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut2);
        String metricsStr2 = statsOut2.toString();
        Multimap<String, Metric> metrics2 = parseMetrics(metricsStr2);
        assertTrue(metrics2.containsKey("pulsar_publish_rate_limit_times"));
        metrics2.get("pulsar_publish_rate_limit_times").forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 0);
            }
        });

        producer.close();
        producer2.close();
        producer3.close();
    }

    @Test
    public void testBrokerMetrics() throws Exception {
        cleanup();
        conf.setAdditionalSystemCursorNames(Set.of("test-cursor"));
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        setup();

        admin.tenants().createTenant("test-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("test-tenant/test-ns", 4);
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://test-tenant/test-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://test-tenant/test-ns/my-topic2").create();
        // system topic
        Producer<byte[]> p3 = pulsarClient.newProducer().topic(
                "persistent://test-tenant/test-ns/__test-topic").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://test-tenant/test-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        // additional system cursor
        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://test-tenant/test-ns/my-topic2")
                .subscriptionName("test-cursor")
                .subscribe();

        Consumer<byte[]> c3 = pulsarClient.newConsumer()
                .topic("persistent://test-tenant/test-ns/__test-topic")
                .subscriptionName("test-v1")
                .subscribe();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
            p3.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
            c3.acknowledge(c3.receive());
        }

        // unsubscribe to test remove cursor impact on metric
        c1.unsubscribe();
        c2.unsubscribe();

        admin.topicPolicies().setRetention("persistent://test-tenant/test-ns/my-topic2",
                        new RetentionPolicies(60, 1024));

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        List<Metric> bytesOutTotal = (List<Metric>) metrics.get("pulsar_broker_out_bytes_total");
        List<Metric> bytesInTotal = (List<Metric>) metrics.get("pulsar_broker_in_bytes_total");
        List<Metric> topicLevelBytesOutTotal = (List<Metric>) metrics.get("pulsar_out_bytes_total");

        assertEquals(bytesOutTotal.size(), 2);
        assertEquals(bytesInTotal.size(), 2);
        assertEquals(topicLevelBytesOutTotal.size(), 3);

        double systemOutBytes = 0.0;
        double userOutBytes = 0.0;
        double systemInBytes = 0.0;
        double userInBytes = 0.0;

        for (Metric metric : bytesOutTotal) {
            if (metric.tags.get("system_subscription").equals("true")) {
                systemOutBytes = metric.value;
            } else {
                userOutBytes = metric.value;
            }
        }

        for (Metric metric : bytesInTotal) {
            if (metric.tags.get("system_topic").equals("true")) {
                systemInBytes = metric.value;
            } else {
                userInBytes = metric.value;
            }
        }

        double systemCursorOutBytes = 0.0;
        for (Metric metric : topicLevelBytesOutTotal) {
            if (metric.tags.get("subscription").startsWith(SystemTopicNames.SYSTEM_READER_PREFIX)) {
                systemCursorOutBytes = metric.value;
            }
        }

        assertEquals(systemCursorOutBytes, systemInBytes);
        assertEquals(userOutBytes / 2, systemOutBytes - systemCursorOutBytes);
        assertEquals(userOutBytes + systemOutBytes, userInBytes + systemInBytes);
    }

    @Test
    public void testMetricsTopicCount() throws Exception {
        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String baseTopic2 = "persistent://" + ns2 + "/testMetricsTopicCount";
        for (int i = 0; i < 6; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic1 + UUID.randomUUID());
        }
        for (int i = 0; i < 3; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic2 + UUID.randomUUID());
        }
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Collection<Metric> metric = metrics.get("pulsar_topics_count");
        metric.forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 6.0);
            }
            if (ns2.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 3.0);
            }
        });
        Collection<Metric> pulsarTopicLoadTimesMetrics = metrics.get("pulsar_topic_load_times");
        Collection<Metric> pulsarTopicLoadTimesCountMetrics = metrics.get("pulsar_topic_load_times_count");
        assertEquals(pulsarTopicLoadTimesMetrics.size(), 6);
        assertEquals(pulsarTopicLoadTimesCountMetrics.size(), 1);
        Collection<Metric> topicLoadTimeP999Metrics = metrics.get("pulsar_topic_load_time_99_9_percentile_ms");
        Collection<Metric> topicLoadTimeFailedCountMetrics = metrics.get("pulsar_topic_load_failed_count");
        assertEquals(topicLoadTimeP999Metrics.size(), 1);
        assertEquals(topicLoadTimeFailedCountMetrics.size(), 1);
    }

    @Test
    public void testMetricsAvgMsgSize2() throws Exception {
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1, 1);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String topicName = baseTopic1 + UUID.randomUUID();
        Producer producer = pulsarClient.newProducer().producerName("my-pub")
                .topic(topicName).create();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).get().get();
        org.apache.pulsar.broker.service.Producer producerInServer = persistentTopic.getProducers().get("my-pub");
        producerInServer.getStats().msgRateIn = 10;
        producerInServer.getStats().msgThroughputIn = 100;
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, true, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        assertTrue(metrics.containsKey("pulsar_average_msg_size"));
        assertEquals(metrics.get("pulsar_average_msg_size").size(), 1);
        Collection<Metric> avgMsgSizes = metrics.get("pulsar_average_msg_size");
        avgMsgSizes.forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.value, 10);
            }
        });
        producer.close();
    }

    @Test
    public void testPerTopicStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 2 metrics with different tags for each topic
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_topic_load_times_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("topic_load_failed_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("subscription"), "test");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("subscription"), "test");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerBrokerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        Collection<Metric> brokerMetrics = metrics.get("pulsar_broker_topics_count");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_subscriptions_count");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_producers_count");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_consumers_count");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_rate_in");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_rate_out");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_throughput_in");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_throughput_out");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_storage_size");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_storage_logical_size");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_storage_write_rate");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_storage_read_rate");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        brokerMetrics = metrics.get("pulsar_broker_msg_backlog");
        assertEquals(brokerMetrics.size(), 1);
        assertEquals(brokerMetrics.stream().toList().get(0).tags.get("cluster"), "test");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    /**
     * Test that the total message and byte counts for a topic are not reset when a consumer disconnects.
     *
     * @throws Exception
     */
    @Test
    public void testPerTopicStatsReconnect() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        final int messages = 5;
        final int pulsarMessageOverhead = 31; // Number of extra bytes pulsar adds to each message
        final int messageSizeBytes = "my-message-n".getBytes().length + pulsarMessageOverhead;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
        }

        c1.close();

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
        }

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        for (int i = 0; i < messages; i++) {
            c2.acknowledge(c2.receive());
        }

        p1.close();
        c2.close();

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, (messageSizeBytes * messages * 2));
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, (messages * 2));
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, (messageSizeBytes * messages * 2));
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, (messages * 2));
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("subscription"), "test");
    }

    @DataProvider(name = "cacheEnable")
    public static Object[][] cacheEnable() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(dataProvider = "cacheEnable")
    public void testStorageReadCacheMissesRate(boolean cacheEnable) throws Exception {
        cleanup();
        conf.setManagedLedgerStatsPeriodSeconds(Integer.MAX_VALUE);
        conf.setManagedLedgerCacheEvictionTimeThresholdMillis(Long.MAX_VALUE);
        conf.setCacheEvictionByMarkDeletedPosition(true);
        if (cacheEnable) {
            conf.setManagedLedgerCacheSizeMB(1);
        } else {
            conf.setManagedLedgerCacheSizeMB(0);
        }
        setup();
        String ns = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns);
        String topic = "persistent://" + ns + "/testStorageReadCacheMissesRate" + UUID.randomUUID();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().enableBatching(false).topic(topic).create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test")
                .receiverQueueSize(0)
                .subscribe();
        byte[] msg = new byte[2 * 1024 * 1024];
        new Random().nextBytes(msg);
        // Block any cache evictions since the test will be flaky unless the test can control when the cache is evicted.
        Runnable triggerPendingCacheEviction = ((ManagedLedgerFactoryImpl) pulsar.getDefaultManagedLedgerFactory())
                .blockPendingCacheEvictions();
        log.info("Sending first message");
        producer.send(msg);
        log.info("Receiving first message");
        Message<byte[]> message = consumer.receive();
        log.info("Sending second message");
        producer.send(msg);
        // trigger pending cache evictions
        triggerPendingCacheEviction.run();
        consumer.acknowledge(message);
        log.info("Receiving second message");
        // when cacheEnable, the second msg will read cache miss
        message = consumer.receive();
        consumer.acknowledge(message);

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        ManagedLedgerImpl managedLedger = ((ManagedLedgerImpl) persistentTopic.getManagedLedger());
        managedLedger.getMbean().refreshStats(1, TimeUnit.SECONDS);

        // includeTopicMetric true
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_read_cache_misses_rate");
        assertEquals(cm.size(), 1);
        if (cacheEnable) {
            assertEquals(cm.get(0).value, 1.0);
        } else {
            assertEquals(cm.get(0).value, 2.0);
        }

        assertEquals(cm.get(0).tags.get("topic"), topic);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        List<Metric> brokerMetric = (List<Metric>) metrics.get("pulsar_broker_storage_read_cache_misses_rate");
        assertEquals(brokerMetric.size(), 1);
        if (cacheEnable) {
            assertEquals(brokerMetric.get(0).value, 1.0);
        } else {
            assertEquals(brokerMetric.get(0).value, 2.0);
        }

        assertEquals(brokerMetric.get(0).tags.get("cluster"), "test");
        assertNull(brokerMetric.get(0).tags.get("namespace"));
        assertNull(brokerMetric.get(0).tags.get("topic"));

        // includeTopicMetric false
        ByteArrayOutputStream statsOut2 = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut2);
        String metricsStr2 = statsOut2.toString();
        Multimap<String, Metric> metrics2 = parseMetrics(metricsStr2);

        metrics2.entries().forEach(e -> System.out.println(e.getKey() + ": " + e.getValue()));

        List<Metric> cm2 = (List<Metric>) metrics2.get("pulsar_storage_read_cache_misses_rate");
        assertEquals(cm2.size(), 1);
        if (cacheEnable) {
            assertEquals(cm2.get(0).value, 1.0);
        } else {
            assertEquals(cm2.get(0).value, 2.0);
        }

        assertNull(cm2.get(0).tags.get("topic"));
        assertEquals(cm2.get(0).tags.get("namespace"), ns);
        assertEquals(cm2.get(0).tags.get("cluster"), "test");

        List<Metric> brokerMetric2 = (List<Metric>) metrics.get("pulsar_broker_storage_read_cache_misses_rate");
        assertEquals(brokerMetric2.size(), 1);
        if (cacheEnable) {
            assertEquals(brokerMetric2.get(0).value, 1.0);
        } else {
            assertEquals(brokerMetric2.get(0).value, 2.0);
        }
        assertEquals(brokerMetric2.get(0).tags.get("cluster"), "test");
        assertNull(brokerMetric2.get(0).tags.get("namespace"));
        assertNull(brokerMetric2.get(0).tags.get("topic"));

        // test ManagedLedgerMetrics
        List<Metric> mlMetric = ((List<Metric>) metrics.get("pulsar_ml_ReadEntriesOpsCacheMissesRate"));
        assertEquals(mlMetric.size(), 1);
        if (cacheEnable) {
            assertEquals(mlMetric.get(0).value, 1.0);
        } else {
            assertEquals(mlMetric.get(0).value, 2.0);
        }
        assertEquals(mlMetric.get(0).tags.get("cluster"), "test");
        assertEquals(mlMetric.get(0).tags.get("namespace"), ns + "/persistent");
    }

    @Test
    public void testPerTopicExpiredStat() throws Exception {
        String ns = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns);
        String topic1 = "persistent://" + ns + "/testPerTopicExpiredStat1";
        String topic2 = "persistent://" + ns + "/testPerTopicExpiredStat2";
        List<String> topicList = Arrays.asList(topic2, topic1);
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic1).create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic(topic2).create();
        final String subName = "test";
        for (String topic : topicList) {
            pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionName(subName)
                    .subscribe().close();
        }

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        p1.close();
        p2.close();
        // Let the message expire
        for (String topic : topicList) {
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopicIfExists(topic).get().get();
            persistentTopic.getBrokerService().getPulsar().getConfiguration().setTtlDurationDefaultInSeconds(-1);
            persistentTopic.getHierarchyTopicPolicies().getMessageTTLInSeconds().updateBrokerValue(-1);
        }
        pulsar.getBrokerService().forEachTopic(Topic::checkMessageExpiry);
        //wait for checkMessageExpiry
        PersistentSubscription sub = (PersistentSubscription)
                pulsar.getBrokerService().getTopicIfExists(topic1).get().get().getSubscription(subName);
        PersistentSubscription sub2 = (PersistentSubscription)
                pulsar.getBrokerService().getTopicIfExists(topic2).get().get().getSubscription(subName);
        Awaitility.await().until(() -> sub.getExpiryMonitor().getTotalMessageExpired() != 0);
        Awaitility.await().until(() -> sub2.getExpiryMonitor().getTotalMessageExpired() != 0);
        sub.getExpiryMonitor().updateRates();
        sub2.getExpiryMonitor().updateRates();
        Awaitility.await().until(() -> sub.getExpiredMessageRate() != 0.0);
        Awaitility.await().until(() -> sub2.getExpiredMessageRate() != 0.0);

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        // There should be 2 metrics with different tags for each topic
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_subscription_last_expire_timestamp");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), topic2);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(1).tags.get("topic"), topic1);
        assertEquals(cm.get(1).tags.get("namespace"), ns);

        //check value
        Field field = PersistentSubscription.class.getDeclaredField("lastExpireTimestamp");
        field.setAccessible(true);
        for (int i = 0; i < topicList.size(); i++) {
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicIfExists(topicList.get(i)).get().get().getSubscription(subName);
            assertEquals((long) field.get(subscription), (long) cm.get(i).value);
        }

        cm = (List<Metric>) metrics.get("pulsar_subscription_msg_rate_expired");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), topic2);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(1).tags.get("topic"), topic1);
        assertEquals(cm.get(1).tags.get("namespace"), ns);
        //check value
        field = PersistentSubscription.class.getDeclaredField("expiryMonitor");
        field.setAccessible(true);
        for (int i = 0; i < topicList.size(); i++) {
            PersistentSubscription subscription = (PersistentSubscription) pulsar.getBrokerService()
                    .getTopicIfExists(topicList.get(i)).get().get().getSubscription(subName);
            PersistentMessageExpiryMonitor monitor = (PersistentMessageExpiryMonitor) field.get(subscription);
            BigDecimal bigDecimal = BigDecimal.valueOf(monitor.getMessageExpiryRate());
            bigDecimal = bigDecimal.setScale(3, RoundingMode.DOWN);
            assertEquals(bigDecimal.doubleValue(), cm.get(i).value);
        }

        cm = (List<Metric>) metrics.get("pulsar_subscription_total_msg_expired");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), topic2);
        assertEquals(cm.get(0).tags.get("namespace"), ns);
        assertEquals(cm.get(1).tags.get("topic"), topic1);
        assertEquals(cm.get(1).tags.get("namespace"), ns);
        //check value
        for (int i = 0; i < topicList.size(); i++) {
            assertEquals(messages, (long) cm.get(i).value);
        }

    }

    @Test
    public void testBundlesMetrics() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
        }

        // Mock another broker to make split task work.
        String mockedBroker = "/loadbalance/brokers/127.0.0.1:0";
        mockZooKeeper.create(mockedBroker, new byte[]{0}, Collections.emptyList(), CreateMode.EPHEMERAL);

        pulsar.getBrokerService().updateRates();
        Awaitility.await().until(() -> !pulsar.getBrokerService().getBundleStats().isEmpty());
        ModularLoadManagerWrapper loadManager = (ModularLoadManagerWrapper) pulsar.getLoadManager().get();
        loadManager.getLoadManager().updateLocalBrokerData();
        // Force registration of UnloadManager load balance stats
        for (var latencyMetric : UnloadManager.LatencyMetric.values()) {
            var serviceUnit = "serviceUnit";
            var brokerLookupAddress = "lookupAddress";
            var serviceUnitStateData = mock(ServiceUnitStateData.class);
            when(serviceUnitStateData.sourceBroker()).thenReturn(brokerLookupAddress);
            when(serviceUnitStateData.dstBroker()).thenReturn(brokerLookupAddress);
            latencyMetric.beginMeasurement(serviceUnit, brokerLookupAddress, serviceUnitStateData);
            latencyMetric.endMeasurement(serviceUnit);
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        assertTrue(metrics.containsKey("pulsar_bundle_msg_rate_in"));
        assertTrue(metrics.containsKey("pulsar_bundle_msg_rate_out"));
        assertTrue(metrics.containsKey("pulsar_bundle_topics_count"));
        assertTrue(metrics.containsKey("pulsar_bundle_consumer_count"));
        assertTrue(metrics.containsKey("pulsar_bundle_producer_count"));
        assertTrue(metrics.containsKey("pulsar_bundle_msg_throughput_in"));
        assertTrue(metrics.containsKey("pulsar_bundle_msg_throughput_out"));

        assertTrue(metrics.containsKey("pulsar_lb_cpu_usage"));
        assertTrue(metrics.containsKey("pulsar_lb_memory_usage"));
        assertTrue(metrics.containsKey("pulsar_lb_directMemory_usage"));
        assertTrue(metrics.containsKey("pulsar_lb_bandwidth_in_usage"));
        assertTrue(metrics.containsKey("pulsar_lb_bandwidth_out_usage"));

        assertTrue(metrics.containsKey("pulsar_lb_bundles_split_total"));

        assertTrue(metrics.containsKey("brk_lb_unload_latency_ms_bucket"));
        assertTrue(metrics.containsKey("brk_lb_release_latency_ms_bucket"));
        assertTrue(metrics.containsKey("brk_lb_assign_latency_ms_bucket"));
        assertTrue(metrics.containsKey("brk_lb_disconnect_latency_ms_bucket"));

        // cleanup.
        mockZooKeeper.delete(mockedBroker, 0);
    }

    @Test
    public void testNonPersistentSubMetrics() throws Exception {
        Producer<byte[]> p1 =
                pulsarClient.newProducer().topic("non-persistent://my-property/use/my-ns/my-topic1").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("non-persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        final int messages = 100;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        assertTrue(metrics.containsKey("pulsar_subscription_back_log"));
        assertTrue(metrics.containsKey("pulsar_subscription_back_log_no_delayed"));
        assertTrue(metrics.containsKey("pulsar_subscription_msg_throughput_out"));
        assertTrue(metrics.containsKey("pulsar_throughput_out"));
        assertTrue(metrics.containsKey("pulsar_subscription_msg_rate_redeliver"));
        assertTrue(metrics.containsKey("pulsar_subscription_unacked_messages"));
        assertTrue(metrics.containsKey("pulsar_subscription_blocked_on_unacked_messages"));
        assertTrue(metrics.containsKey("pulsar_subscription_msg_rate_out"));
        assertTrue(metrics.containsKey("pulsar_out_bytes_total"));
        assertTrue(metrics.containsKey("pulsar_out_messages_total"));
        assertTrue(metrics.containsKey("pulsar_subscription_last_expire_timestamp"));
        assertTrue(metrics.containsKey("pulsar_subscription_msg_drop_rate"));
        assertTrue(metrics.containsKey("pulsar_subscription_consumers_count"));
    }

    @Test
    public void testPerNamespaceStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 1);
        assertNull(cm.get(0).tags.get("topic"));
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 1);
        assertNull(cm.get(0).tags.get("topic"));
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_in_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerProducerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .producerName("producer1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2")
                .producerName("producer2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("Test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("Test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, true, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_producer_msg_rate_in");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("producer_name"), "producer1");
        assertEquals(cm.get(0).tags.get("producer_id"), "0");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("producer_name"), "producer2");
        assertEquals(cm.get(1).tags.get("producer_id"), "1");

        cm = (List<Metric>) metrics.get("pulsar_producer_msg_throughput_in");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("producer_name"), "producer1");
        assertEquals(cm.get(0).tags.get("producer_id"), "0");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(1).tags.get("producer_name"), "producer2");
        assertEquals(cm.get(1).tags.get("producer_id"), "1");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    @Test
    public void testPerConsumerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();

        Consumer<byte[]> c1 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("test")
                .subscribe();

        Consumer<byte[]> c2 = pulsarClient.newConsumer()
                .topic("persistent://my-property/use/my-ns/my-topic2")
                .subscriptionName("test")
                .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, true, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_out_bytes_total");
        assertEquals(cm.size(), 4);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("consumer_id"), "0");

        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(2).tags.get("subscription"), "test");

        assertEquals(cm.get(3).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(3).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(3).tags.get("subscription"), "test");
        assertEquals(cm.get(3).tags.get("consumer_id"), "1");

        cm = (List<Metric>) metrics.get("pulsar_out_messages_total");
        assertEquals(cm.size(), 4);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(0).tags.get("subscription"), "test");

        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("subscription"), "test");
        assertEquals(cm.get(1).tags.get("consumer_id"), "0");

        assertEquals(cm.get(2).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(2).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(2).tags.get("subscription"), "test");

        assertEquals(cm.get(3).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(3).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(3).tags.get("subscription"), "test");
        assertEquals(cm.get(3).tags.get("consumer_id"), "1");

        p1.close();
        p2.close();
        c1.close();
        c2.close();
    }

    /** Checks for duplicate type definitions for a metric in the Prometheus metrics output. If the Prometheus parser
     finds a TYPE definition for the same metric more than once, it errors out:
     https://github.com/prometheus/prometheus/blob/f04b1b5559a80a4fd1745cf891ce392a056460c9/vendor/github.com/
     prometheus/common/expfmt/text_parse.go#L499-L502
     This can happen when including topic metrics,
     since the same metric is reported multiple times with different labels. For example:

     # TYPE pulsar_subscriptions_count gauge
     pulsar_subscriptions_count{cluster="standalone"} 0
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",
     topic="persistent://public/functions/metadata"} 1.0
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",
     topic="persistent://public/functions/coordinate"} 1.0
     pulsar_subscriptions_count{cluster="standalone",namespace="public/functions",
     topic="persistent://public/functions/assignments"} 1.0

     **/
    // Running the test twice to make sure types are present when generated multiple times
    @Test(invocationCount = 2)
    public void testDuplicateMetricTypeDefinitions() throws Exception {
        cleanup();
        conf.setTransactionCoordinatorEnabled(true);
        conf.setTransactionLogBatchedWriteEnabled(true);
        conf.setTransactionPendingAckBatchedWriteEnabled(true);
        setup();

        Set<String> allPrometheusSuffixString = allPrometheusSuffixEnums();
        Producer<byte[]> p1 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Map<String, String> typeDefs = new HashMap<>();
        Map<String, String> metricNames = new HashMap<>();

        Pattern typePattern = Pattern.compile("^#\\s+TYPE\\s+(\\w+)\\s+(\\w+)");
        Pattern metricNamePattern = Pattern.compile("^(\\w+)\\{.+");

        Splitter.on("\n").split(metricsStr).forEach(line -> {
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("#")) {
                // Check for duplicate type definitions
                Matcher typeMatcher = typePattern.matcher(line);
                checkArgument(typeMatcher.matches());
                String metricName = typeMatcher.group(1);
                String type = typeMatcher.group(2);

                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "Only one TYPE line may exist for a given metric name."
                if (!typeDefs.containsKey(metricName)) {
                    typeDefs.put(metricName, type);
                } else {
                    fail("Duplicate type definition found for TYPE definition " + metricName);
                    System.out.println(metricsStr);

                }
                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "The TYPE line for a metric name must appear before the first sample is reported
                // for that metric name."
                if (metricNames.containsKey(metricName)) {
                    System.out.println(metricsStr);
                    fail("TYPE definition for " + metricName + " appears after first sample");

                }
            } else {
                Matcher metricMatcher = metricNamePattern.matcher(line);
                checkArgument(metricMatcher.matches());
                String metricName = metricMatcher.group(1);
                metricNames.put(metricName, metricName);
            }
        });

        // Metrics with no type definition
        for (String metricName : metricNames.keySet()) {

            if (!typeDefs.containsKey(metricName)) {
                // This may be OK if this is a _sum or _count metric from a summary
                boolean isNorm = false;
                for (String suffix : allPrometheusSuffixString){
                    if (metricName.endsWith(suffix)){
                        String summaryMetricName = metricName.substring(0, metricName.indexOf(suffix));
                        if (!typeDefs.containsKey(summaryMetricName)) {
                            fail("Metric " + metricName + " does not have a corresponding summary type definition");
                        }
                        isNorm = true;
                        break;
                    }
                }
                if (!isNorm){
                    fail("Metric " + metricName + " does not have a type definition");
                }

            }
        }

        p1.close();
        p2.close();

        conf.setTransactionCoordinatorEnabled(false);
        conf.setTransactionLogBatchedWriteEnabled(false);
        conf.setTransactionPendingAckBatchedWriteEnabled(false);
    }

    /***
     * this method will return ["_sum", "_info", "_bucket", "_count", "_total", "_created", "_gsum", "_gcount"].
     */
    public static Set<String> allPrometheusSuffixEnums(){
        HashSet<String> result = new HashSet<>();
        final String metricsName = "123";
        for (Collector.Type type : Collector.Type.values()){
            Collector.MetricFamilySamples metricFamilySamples =
                    new Collector.MetricFamilySamples(metricsName, type, "", new ArrayList<>());
            result.addAll(Arrays.asList(metricFamilySamples.getNames()));
        }
        return result.stream()
                .map(str -> str.substring(metricsName.length()))
                .filter(str -> StringUtils.isNotBlank(str))
                .collect(Collectors.toSet());
    }


    @Test
    public void testManagedLedgerCacheStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_cache_evictions");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get("pulsar_ml_cache_hits_rate");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        p1.close();
        p2.close();
    }

    @Test
    public void testManagedLedgerStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2").create();
        Producer<byte[]> p3 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns2/my-topic1").create();
        Producer<byte[]> p4 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns2/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
            p3.send(message.getBytes());
            p4.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        Map<String, String> typeDefs = new HashMap<>();
        Map<String, String> metricNames = new HashMap<>();

        Pattern typePattern = Pattern.compile("^#\\s+TYPE\\s+(\\w+)\\s+(\\w+)");
        Pattern metricNamePattern = Pattern.compile("^(\\w+)\\{.+");

        Splitter.on("\n").split(metricsStr).forEach(line -> {
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("#")) {
                // Check for duplicate type definitions
                Matcher typeMatcher = typePattern.matcher(line);
                checkArgument(typeMatcher.matches());
                String metricName = typeMatcher.group(1);
                String type = typeMatcher.group(2);

                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "Only one TYPE line may exist for a given metric name."
                if (!typeDefs.containsKey(metricName)) {
                    typeDefs.put(metricName, type);
                } else {
                    fail("Duplicate type definition found for TYPE definition " + metricName);
                }
                // From https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md
                // "The TYPE line for a metric name must appear before the first sample is reported
                // for that metric name."
                if (metricNames.containsKey(metricName)) {
                    fail("TYPE definition for " + metricName + " appears after first sample");
                }
            } else {
                Matcher metricMatcher = metricNamePattern.matcher(line);
                checkArgument(metricMatcher.matches());
                String metricName = metricMatcher.group(1);
                metricNames.put(metricName, metricName);
            }
        });

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_AddEntryBytesRate");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        String ns = cm.get(0).tags.get("namespace");
        assertTrue(ns.equals("my-property/use/my-ns") || ns.equals("my-property/use/my-ns2"));

        cm = (List<Metric>) metrics.get("pulsar_ml_AddEntryMessagesRate");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        ns = cm.get(0).tags.get("namespace");
        assertTrue(ns.equals("my-property/use/my-ns") || ns.equals("my-property/use/my-ns2"));

        p1.close();
        p2.close();
        p3.close();
        p4.close();
    }

    @Test
    public void testManagedLedgerBookieClientStats() throws Exception {
        @Cleanup
        Producer<byte[]> p1 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1").create();

        @Cleanup
        Producer<byte[]> p2 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e ->
                System.out.println(e.getKey() + ": " + e.getValue())
        );

        List<Metric> cm = (List<Metric>) metrics.get(
                keyNameBySubstrings(metrics,
                        "pulsar_managedLedger_client", "bookkeeper_ml_scheduler_threads"));
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get(
                keyNameBySubstrings(metrics,
                        "pulsar_managedLedger_client", "bookkeeper_ml_scheduler_task_execution_sum"));
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        cm = (List<Metric>) metrics.get(
                keyNameBySubstrings(metrics,
                        "pulsar_managedLedger_client", "bookkeeper_ml_scheduler_max_queue_size"));
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
    }

    private static String keyNameBySubstrings(Multimap<String, Metric> metrics, String... substrings) {
        for (String key: metrics.keys()) {
            boolean found = true;
            for (String s: substrings) {
                if (!key.contains(s)) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return key;
            }
        }
        return null;
    }

    @Test
    public void testAuthMetrics() throws IOException, AuthenticationException {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(AuthenticationProvider.Context.builder().config(conf).build());

        try {
            provider.authenticate(new AuthenticationDataSource() {
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, no credential passed
        }

        String token = AuthTokenUtils.createToken(secretKey, "subject", Optional.empty());

        // Pulsar protocol auth
        String subject = provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_authentication_success_total");
        boolean haveSucceed = false;
        for (Metric metric : cm) {
            if (Objects.equals(metric.tags.get("auth_method"), "token")
                    && Objects.equals(metric.tags.get("provider_name"), provider.getClass().getSimpleName())) {
                haveSucceed = true;
            }
        }
        Assert.assertTrue(haveSucceed);

        cm = (List<Metric>) metrics.get("pulsar_authentication_failures_total");

        boolean haveFailed = false;
        for (Metric metric : cm) {
            if (Objects.equals(metric.tags.get("auth_method"), "token")
                    && Objects.equals(metric.tags.get("reason"),
                    AuthenticationProviderToken.ErrorCode.INVALID_AUTH_DATA.name())
                    && Objects.equals(metric.tags.get("provider_name"), provider.getClass().getSimpleName())) {
                haveFailed = true;
            }
        }
        Assert.assertTrue(haveFailed);
    }

    @Test
    public void testExpiredTokenMetrics() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(AuthenticationProvider.Context.builder().config(conf).build());

        Date expiredDate = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        String expiredToken = AuthTokenUtils.createToken(secretKey, "subject", Optional.of(expiredDate));

        try {
            provider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return expiredToken;
                }
            });
            fail("Should have failed");
        } catch (AuthenticationException e) {
            // expected, token was expired
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_expired_token_total");
        assertEquals(cm.size(), 1);

        provider.close();
    }

    @Test
    public void testExpiringTokenMetrics() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        @Cleanup
        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        provider.initialize(AuthenticationProvider.Context.builder().config(conf).build());

        int[] tokenRemainTime = new int[]{3, 7, 40, 100, 400};

        for (int remainTime : tokenRemainTime) {
            Date expiredDate = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(remainTime));
            String expiringToken = AuthTokenUtils.createToken(secretKey, "subject", Optional.of(expiredDate));
            provider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return expiringToken;
                }
            });
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Metric countMetric = ((List<Metric>) metrics.get("pulsar_expiring_token_minutes_count")).get(0);
        assertEquals(countMetric.value, tokenRemainTime.length);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_expiring_token_minutes_bucket");
        var buckets = cm.stream().map(m -> m.tags.get("le")).collect(Collectors.toSet());
        assertThat(buckets).isEqualTo(Set.of("5.0", "10.0", "60.0", "240.0", "1440.0", "10080.0",
                "20160.0", "43200.0", "129600.0", "259200.0", "388800.0", "525600.0", "+Inf"));
        cm.forEach((e) -> {
            var expectedValue = switch(e.tags.get("le")) {
                case "5.0" -> 1;
                case "10.0" -> 2;
                case "60.0" -> 3;
                case "240.0" -> 4;
                default -> 5;
            };
            assertEquals(e.value, expectedValue);
        });
    }

    @Test
    public void testParsingWithPositiveInfinityValue() {
        Multimap<String, Metric> metrics =
                parseMetrics("pulsar_broker_publish_latency{cluster=\"test\",quantile=\"0.0\"} +Inf");
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_broker_publish_latency");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("quantile"), "0.0");
        assertEquals(cm.get(0).value, Double.POSITIVE_INFINITY);
    }

    @Test
    public void testParsingWithNegativeInfinityValue() {
        Multimap<String, Metric> metrics =
                parseMetrics("pulsar_broker_publish_latency{cluster=\"test\",quantile=\"0.0\"} -Inf");
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_broker_publish_latency");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("quantile"), "0.0");
        assertEquals(cm.get(0).value, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testManagedCursorPersistStats() throws Exception {
        final String subName = "my-sub";
        final String topicName = "persistent://my-namespace/use/my-ns/my-topic1";
        final int messageSize = 10;

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        for (int i = 0; i < messageSize; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            consumer.acknowledge(consumer.receive().getMessageId());
        }

        // enable ExposeManagedCursorMetricsInPrometheus
        pulsar.getConfiguration().setExposeManagedCursorMetricsInPrometheus(true);
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        List<Metric> cm = (List<Metric>) metrics.get("pulsar_ml_cursor_persistLedgerSucceed");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("cursor_name"), subName);

        // disable ExposeManagedCursorMetricsInPrometheus
        pulsar.getConfiguration().setExposeManagedCursorMetricsInPrometheus(false);
        ByteArrayOutputStream statsOut2 = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut2);
        String metricsStr2 = statsOut2.toString();
        Multimap<String, Metric> metrics2 = parseMetrics(metricsStr2);
        List<Metric> cm2 = (List<Metric>) metrics2.get("pulsar_ml_cursor_persistLedgerSucceed");
        assertEquals(cm2.size(), 0);

        producer.close();
        consumer.close();
    }

    @Test
    public void testBrokerConnection() throws Exception {
        final String topicName = "persistent://my-namespace/use/my-ns/my-topic1";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_connection_created_total_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_create_success_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_closed_total_count");
        compareBrokerConnectionStateCount(cm, 0.0);

        cm = (List<Metric>) metrics.get("pulsar_active_connections");
        compareBrokerConnectionStateCount(cm, 1.0);

        pulsarClient.close();
        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();

        metrics = parseMetrics(metricsStr);
        cm = (List<Metric>) metrics.get("pulsar_connection_closed_total_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        pulsar.getConfiguration().setAuthenticationEnabled(true);

        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .operationTimeout(1, TimeUnit.MILLISECONDS));

        try {
            pulsarClient.newProducer()
                    .topic(topicName)
                    .create();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarClientException.AuthenticationException);
        }

        pulsarClient.close();
        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();

        metrics = parseMetrics(metricsStr);
        cm = (List<Metric>) metrics.get("pulsar_connection_closed_total_count");
        compareBrokerConnectionStateCount(cm, 2.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_create_fail_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_create_success_count");
        compareBrokerConnectionStateCount(cm, 1.0);

        cm = (List<Metric>) metrics.get("pulsar_active_connections");
        compareBrokerConnectionStateCount(cm, 0.0);

        cm = (List<Metric>) metrics.get("pulsar_connection_created_total_count");
        compareBrokerConnectionStateCount(cm, 2.0);
    }

    @Test
    public void testBrokerHealthCheckMetric() throws Exception {
        conf.setHealthCheckMetricsUpdateTimeInSeconds(60);
        BrokerService brokerService = pulsar.getBrokerService();
        brokerService.checkHealth().get();
        brokerService.updateRates();
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_health");
        compareBrokerConnectionStateCount(cm, 1);
    }

    private void compareBrokerConnectionStateCount(List<Metric> cm, double count) {
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("cluster"), "test");
        assertEquals(cm.get(0).tags.get("broker"), "localhost");
        assertEquals(cm.get(0).value, count);
    }

    @Test
    void testParseMetrics() throws IOException {
        String sampleMetrics = IOUtils.toString(getClass().getClassLoader()
                .getResourceAsStream("prometheus_metrics_sample.txt"), StandardCharsets.UTF_8);
        parseMetrics(sampleMetrics);
    }

    @Test
    public void testCompaction() throws Exception {
        final String topicName = "persistent://my-namespace/use/my-ns/my-compaction1";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_compaction_removed_event_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_succeed_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_failed_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_duration_time_in_mills");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_read_throughput");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_write_throughput");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_count");
        assertEquals(cm.size(), 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_size");
        assertEquals(cm.size(), 0);
        //
        final int numMessages = 1000;
        final int maxKeys = 10;
        Random r = new Random(0);
        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage()
                    .key(key)
                    .value(data)
                    .send();
        }
        Compactor compactor = ((PulsarCompactionServiceFactory) pulsar.getCompactionServiceFactory()).getCompactor();
        compactor.compact(topicName).get();
        statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        metricsStr = statsOut.toString();
        metrics = parseMetrics(metricsStr);
        cm = (List<Metric>) metrics.get("pulsar_compaction_removed_event_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 990);
        cm = (List<Metric>) metrics.get("pulsar_compaction_succeed_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 1);
        cm = (List<Metric>) metrics.get("pulsar_compaction_failed_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_duration_time_in_mills");
        assertEquals(cm.size(), 1);
        assertTrue(cm.get(0).value > 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_read_throughput");
        assertEquals(cm.size(), 1);
        assertTrue(cm.get(0).value > 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_write_throughput");
        assertEquals(cm.size(), 1);
        assertTrue(cm.get(0).value > 0);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 10);
        cm = (List<Metric>) metrics.get("pulsar_compaction_compacted_entries_size");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 840);

        pulsarClient.close();
    }

    @Test
    public void testMetricsWithCache() throws Throwable {
        ServiceConfiguration configuration = pulsar.getConfiguration();
        configuration.setManagedLedgerStatsPeriodSeconds(2);
        configuration.setMetricsBufferResponse(true);
        configuration.setClusterName(configClusterName);

        // create a mock clock to control the time
        AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());
        Clock clock = mock();
        when(clock.millis()).thenAnswer(invocation -> currentTimeMillis.get());

        @Cleanup
        PrometheusMetricsGenerator prometheusMetricsGenerator =
                new PrometheusMetricsGenerator(pulsar, true, false, false,
                        false, clock);
        String previousMetrics = null;
        for (int a = 0; a < 4; a++) {
            ByteArrayOutputStream statsOut1 = new ByteArrayOutputStream();
            PrometheusMetricsTestUtil.generate(prometheusMetricsGenerator, statsOut1, null);
            ByteArrayOutputStream statsOut2 = new ByteArrayOutputStream();
            PrometheusMetricsTestUtil.generate(prometheusMetricsGenerator, statsOut2, null);

            String metricsStr1 = statsOut1.toString();
            String metricsStr2 = statsOut2.toString();
            assertTrue(metricsStr1.length() > 1000);
            assertEquals(metricsStr1, metricsStr2);
            assertNotEquals(metricsStr1, previousMetrics);
            previousMetrics = metricsStr1;
            // move time forward
            currentTimeMillis.addAndGet(TimeUnit.SECONDS.toMillis(2));
        }
    }

    @Test
    public void testSplitTopicAndPartitionLabel() throws Exception {
        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        String baseTopic1 = "persistent://" + ns1 + "/testMetricsTopicCount";
        String baseTopic2 = "persistent://" + ns2 + "/testMetricsTopicCount";
        for (int i = 0; i < 6; i++) {
            admin.topics().createNonPartitionedTopic(baseTopic1 + UUID.randomUUID());
        }
        for (int i = 0; i < 3; i++) {
            admin.topics().createPartitionedTopic(baseTopic2 + UUID.randomUUID(), 3);
        }
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topicsPattern("persistent://" + ns1 + "/.*")
                .subscriptionName("sub")
                .subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topicsPattern("persistent://" + ns2 + "/.*")
                .subscriptionName("sub")
                .subscribe();
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, true,  statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);
        Collection<Metric> metric = metrics.get("pulsar_consumers_count");
        assertTrue(metric.size() >= 15);
        metric.forEach(item -> {
            if (ns1.equals(item.tags.get("namespace"))) {
                assertEquals(item.tags.get("partition"), "-1");
            }
            if (ns2.equals(item.tags.get("namespace"))) {
                System.out.println(item);
                assertTrue(Integer.parseInt(item.tags.get("partition")) >= 0);
            }
        });
        consumer1.close();
        consumer2.close();
    }

    @Test
    public void testMetricsGroupedByTypeDefinitions() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer()
                .topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, statsOut);
        String metricsStr = statsOut.toString();

        Pattern typePattern = Pattern.compile("^#\\s+TYPE\\s+(\\w+)\\s+(\\w+)");
        Pattern metricNamePattern = Pattern.compile("^(\\w+)\\{.+");

        AtomicReference<String> currentMetric = new AtomicReference<>();
        Splitter.on("\n").split(metricsStr).forEach(line -> {
            if (line.isEmpty()) {
                return;
            }
            if (line.startsWith("#")) {
                // Get the current type definition
                Matcher typeMatcher = typePattern.matcher(line);
                checkArgument(typeMatcher.matches());
                String metricName = typeMatcher.group(1);
                currentMetric.set(metricName);
            } else {
                Matcher metricMatcher = metricNamePattern.matcher(line);
                checkArgument(metricMatcher.matches());
                String metricName = metricMatcher.group(1);

                if (metricName.endsWith("_bucket")) {
                    metricName = metricName.substring(0, metricName.indexOf("_bucket"));
                } else if (metricName.endsWith("_count") && !currentMetric.get().endsWith("_count")) {
                    metricName = metricName.substring(0, metricName.indexOf("_count"));
                } else if (metricName.endsWith("_sum") && !currentMetric.get().endsWith("_sum")) {
                    metricName = metricName.substring(0, metricName.indexOf("_sum"));
                } else if (metricName.endsWith("_total") && !currentMetric.get().endsWith("_total")) {
                    metricName = metricName.substring(0, metricName.indexOf("_total"));
                } else if (metricName.endsWith("_created") && !currentMetric.get().endsWith("_created")) {
                    metricName = metricName.substring(0, metricName.indexOf("_created"));
                }

                if (!metricName.equals(currentMetric.get())) {
                    System.out.println(metricsStr);
                    fail("Metric not grouped under its type definition: " + line);
                }

            }
        });

        p1.close();
        p2.close();
    }

    @Test
    public void testEscapeLabelValue() throws Exception {
        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1);
        String topic = "persistent://" + ns1 + "/\"mytopic";
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        final Consumer<?> consumer = pulsarClient.newConsumer()
                .subscriptionName("sub")
                .topic(topic)
                .subscribe();
        @Cleanup
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false,
                false, statsOut);
        String metricsStr = statsOut.toString();
        final List<String> subCountLines = metricsStr.lines()
                .filter(line -> line.startsWith("pulsar_subscriptions_count"))
                .collect(Collectors.toList());
        System.out.println(subCountLines);
        assertEquals(subCountLines.size(), 1);
        assertEquals(subCountLines.get(0),
                "pulsar_subscriptions_count{cluster=\"test\",namespace=\"prop/ns-abc1\","
                        + "topic=\"persistent://prop/ns-abc1/\\\"mytopic\"} 1");
    }

}
