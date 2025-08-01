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
package org.apache.pulsar.compaction;

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricDoubleSumValue;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.apache.pulsar.client.impl.RawReaderTest.extractKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class CompactorTest extends MockedPulsarServiceBaseTest {

    protected ScheduledExecutorService compactionScheduler;

    protected BookKeeper bk;
    protected Compactor compactor;


    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compactor").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(
                this.conf, null, null, Optional.empty(), null).get();
        compactor = new PublishingOrderCompactor(conf, pulsarClient, bk, compactionScheduler);
    }


    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        bk.close();
        compactionScheduler.shutdownNow();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    protected long compact(String topic) throws ExecutionException, InterruptedException {
        return compactor.compact(topic).get();
    }

    protected Compactor getCompactor() {
        return compactor;
    }

    protected List<String> compactAndVerify(String topic, Map<String, byte[]> expected, boolean checkMetrics)
            throws Exception {

        long compactedLedgerId = compact(topic);

        LedgerHandle ledger = bk.openLedger(compactedLedgerId,
                Compactor.COMPACTED_TOPIC_LEDGER_DIGEST_TYPE,
                Compactor.COMPACTED_TOPIC_LEDGER_PASSWORD);
        Assert.assertEquals(ledger.getLastAddConfirmed() + 1, // 0..lac
                expected.size(),
                "Should have as many entries as there is keys");

        List<String> keys = new ArrayList<>();
        Enumeration<LedgerEntry> entries = ledger.readEntries(0, ledger.getLastAddConfirmed());
        while (entries.hasMoreElements()) {
            ByteBuf buf = entries.nextElement().getEntryBuffer();
            RawMessage m = RawMessageImpl.deserializeFrom(buf);
            String key = extractKey(m);
            keys.add(key);

            ByteBuf payload = extractPayload(m);
            byte[] bytes = new byte[payload.readableBytes()];
            payload.readBytes(bytes);
            Assert.assertEquals(bytes, expected.remove(key),
                    "Compacted version should match expected version");
            m.close();
        }
        if (checkMetrics) {
            CompactionRecord compactionRecord = getCompactor().getStats().getCompactionRecordForTopic(topic).get();
            long compactedTopicRemovedEventCount = compactionRecord.getLastCompactionRemovedEventCount();
            long lastCompactSucceedTimestamp = compactionRecord.getLastCompactionSucceedTimestamp();
            long lastCompactFailedTimestamp = compactionRecord.getLastCompactionFailedTimestamp();
            long lastCompactDurationTimeInMills = compactionRecord.getLastCompactionDurationTimeInMills();
            Assert.assertTrue(compactedTopicRemovedEventCount >= 1);
            Assert.assertTrue(lastCompactSucceedTimestamp >= 1L);
            Assert.assertTrue(lastCompactDurationTimeInMills >= 0L);
            Assert.assertEquals(lastCompactFailedTimestamp, 0L);
        }
        Assert.assertTrue(expected.isEmpty(), "All expected keys should have been found");
        return keys;
    }

    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 1000;
        final int maxKeys = 10;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, byte[]> expected = new HashMap<>();
        Random r = new Random(0);

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage()
                    .key(key)
                    .value(data)
                    .send();
            expected.put(key, data);
        }
        compactAndVerify(topic, expected, true);
    }

    @Test
    public void testAllCompactedOut() throws Exception {
        String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/use/my-ns/testAllCompactedOut");
        // set retain null key to true
        boolean oldRetainNullKey = pulsar.getConfig().isTopicCompactionRetainNullKey();
        pulsar.getConfig().setTopicCompactionRetainNullKey(true);
        this.restartBroker();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(true).topic(topicName).batchingMaxMessages(3).create();

        producer.newMessage().key("K1").value("V1").sendAsync();
        producer.newMessage().key("K2").value("V2").sendAsync();
        producer.newMessage().key("K2").value(null).sendAsync();
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "my-property")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "my-property/use/my-ns")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName)
                .build();
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_REMOVED_COUNTER, attributes, 1);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_OPERATION_COUNTER, Attributes.builder()
                        .putAll(attributes)
                        .put(OpenTelemetryAttributes.PULSAR_COMPACTION_STATUS, "success")
                        .build(),
                1);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_OPERATION_COUNTER, Attributes.builder()
                        .putAll(attributes)
                        .put(OpenTelemetryAttributes.PULSAR_COMPACTION_STATUS, "failure")
                        .build(),
                0);
        assertMetricDoubleSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_DURATION_SECONDS, attributes,
                actual -> assertThat(actual).isPositive());
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_IN_COUNTER, attributes,
                actual -> assertThat(actual).isPositive());
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_OUT_COUNTER, attributes,
                actual -> assertThat(actual).isPositive());
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_ENTRIES_COUNTER, attributes, 1);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_COUNTER, attributes,
                actual -> assertThat(actual).isPositive());

        producer.newMessage().key("K1").value(null).sendAsync();
        producer.flush();

        admin.topics().triggerCompaction(topicName);

        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(admin.topics().compactionStatus(topicName).status,
                    LongRunningProcessStatus.Status.SUCCESS);
        });

        @Cleanup
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .subscriptionName("reader-test")
                .topic(topicName)
                .readCompacted(true)
                .startMessageId(MessageId.earliest)
                .create();
        while (reader.hasMessageAvailable()) {
            Message<String> message = reader.readNext(3, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
        }
        // set retain null key back to avoid affecting other tests
        pulsar.getConfig().setTopicCompactionRetainNullKey(oldRetainNullKey);
    }

    @Test
    public void testCompactAddCompact() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, byte[]> expected = new HashMap<>();

        producer.newMessage()
                .key("a")
                .value("A_1".getBytes())
                .send();
        producer.newMessage()
                .key("b")
                .value("B_1".getBytes())
                .send();
        producer.newMessage()
                .key("a")
                .value("A_2".getBytes())
                .send();
        expected.put("a", "A_2".getBytes());
        expected.put("b", "B_1".getBytes());

        compactAndVerify(topic, new HashMap<>(expected), false);

        producer.newMessage()
                .key("b")
                .value("B_2".getBytes())
                .send();
        expected.put("b", "B_2".getBytes());

        compactAndVerify(topic, expected, false);
    }

    @Test
    public void testCompactedInOrder() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        producer.newMessage()
                .key("c")
                .value("C_1".getBytes()).send();
        producer.newMessage()
                .key("a")
                .value("A_1".getBytes()).send();
        producer.newMessage()
                .key("b")
                .value("B_1".getBytes()).send();
        producer.newMessage()
                .key("a")
                .value("A_2".getBytes()).send();
        Map<String, byte[]> expected = new HashMap<>();
        expected.put("a", "A_2".getBytes());
        expected.put("b", "B_1".getBytes());
        expected.put("c", "C_1".getBytes());

        List<String> keyOrder = compactAndVerify(topic, expected, false);

        Assert.assertEquals(keyOrder, Lists.newArrayList("c", "b", "a"));
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // trigger creation of topic on server side
        pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe().close();

        compact(topic);
    }

    @Test
    public void testPhaseOneLoopTimeConfiguration() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setBrokerServiceCompactionPhaseOneLoopTimeInSeconds(60);
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(mockClient.getCnxPool()).thenReturn(connectionPool);
        PublishingOrderCompactor compactor = new PublishingOrderCompactor(configuration, mockClient,
                Mockito.mock(BookKeeper.class), compactionScheduler);
        Assert.assertEquals(compactor.getPhaseOneLoopReadTimeoutInSeconds(), 60);
    }

    @Test
    public void testCompactedWithConcurrentSend() throws Exception {
        String topic = "persistent://my-property/use/my-ns/testCompactedWithConcurrentSend";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        var future = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 100; i++) {
                try {
                    producer.newMessage().key(String.valueOf(i)).value(String.valueOf(i).getBytes()).send();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        PulsarTopicCompactionService topicCompactionService =
                (PulsarTopicCompactionService) persistentTopic.getTopicCompactionService();

        Awaitility.await().untilAsserted(() -> {
            long compactedLedgerId = compact(topic);
            Thread.sleep(300);
            Optional<CompactedTopicContext> compactedTopicContext = topicCompactionService.getCompactedTopic()
                    .getCompactedTopicContext();
            Assert.assertTrue(compactedTopicContext.isPresent());
            Assert.assertEquals(compactedTopicContext.get().ledger.getId(), compactedLedgerId);
        });

        Position lastCompactedPosition = topicCompactionService.getLastCompactedPosition().get();
        Entry lastCompactedEntry = topicCompactionService.readLastCompactedEntry().get();

        Assert.assertTrue(PositionFactory.create(lastCompactedPosition.getLedgerId(),
                lastCompactedPosition.getEntryId()).compareTo(lastCompactedEntry.getLedgerId(),
                lastCompactedEntry.getEntryId()) >= 0);

        future.join();
    }

    public ByteBuf extractPayload(RawMessage m) throws Exception {
        ByteBuf payloadAndMetadata = m.getHeadersAndPayload();
        Commands.skipChecksumIfPresent(payloadAndMetadata);
        int metadataSize = payloadAndMetadata.readInt(); // metadata size
         byte[] metadata = new byte[metadataSize];
        payloadAndMetadata.readBytes(metadata);
        return payloadAndMetadata.slice();
    }
}
