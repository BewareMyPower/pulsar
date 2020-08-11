/**
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
#include <pulsar/Client.h>
#include <gtest/gtest.h>

#include "../lib/Future.h"
#include "../lib/Latch.h"
#include "../lib/Utils.h"

#include "HttpHelper.h"

using namespace pulsar;

static std::string lookupUrl = "pulsar://localhost:6650";
static std::string adminV2Url = "http://localhost:8080/admin/v2/";

TEST(ConsumerTest, consumerNotInitialized) {
    Consumer consumer;

    ASSERT_TRUE(consumer.getTopic().empty());
    ASSERT_TRUE(consumer.getSubscriptionName().empty());

    Message msg;
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.receive(msg));
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.receive(msg, 3000));

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledge(msg));

    MessageId msgId;
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledge(msgId));

    Result result;
    {
        Promise<bool, Result> promise;
        consumer.acknowledgeAsync(msg, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeAsync(msgId, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledgeCumulative(msg));
    ASSERT_EQ(ResultConsumerNotInitialized, consumer.acknowledgeCumulative(msgId));

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeCumulativeAsync(msg, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    {
        Promise<bool, Result> promise;
        consumer.acknowledgeCumulativeAsync(msgId, WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.close());

    {
        Promise<bool, Result> promise;
        consumer.closeAsync(WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.unsubscribe());

    {
        Promise<bool, Result> promise;
        consumer.unsubscribeAsync(WaitForCallback(promise));
        promise.getFuture().get(result);

        ASSERT_EQ(ResultConsumerNotInitialized, result);
    }

    const std::string topic = "test-topic";

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.subscribeOneTopic(topic));

    {
        Latch latch(1);
        consumer.subscribeOneTopicAsync(topic, [&latch](Result result) {
            ASSERT_EQ(ResultConsumerNotInitialized, result);
            latch.countdown();
        });
        latch.wait();
    }

    ASSERT_EQ(ResultConsumerNotInitialized, consumer.unsubscribeOneTopic(topic));

    {
        Latch latch(1);
        consumer.unsubscribeOneTopicAsync(topic, [&latch](Result result) {
            ASSERT_EQ(ResultConsumerNotInitialized, result);
            latch.countdown();
        });
        latch.wait();
    }
}

// For a single topic, subscribe or unsubscribe is not allowed, even if it's a partitioned topic
TEST(ConsumerTest, operationNotSupported) {
    const std::string topic1 = "ConsumerSubNotAllowed";
    const std::string topic2 = "ConsumerSubNotAllowed" + std::to_string(time(nullptr));
    const std::string subName = "SubscriptionName";

    int res = makePutRequest(adminV2Url + "persistent/public/default/" + topic2 + "/partitions", "3");
    ASSERT_TRUE(res == 204 || res == 409) << "res = " << res;

    Client client(lookupUrl);
    Consumer consumers[2];
    ASSERT_EQ(ResultOk, client.subscribe(topic1, subName, consumers[0]));
    ASSERT_EQ(ResultOk, client.subscribe(topic2, subName, consumers[1]));

    for (auto& consumer : consumers) {
        ASSERT_EQ(ResultOperationNotSupported, consumer.subscribeOneTopic("AnyTopic"));
        ASSERT_EQ(ResultOperationNotSupported, consumer.unsubscribeOneTopic("AnyTopic"));
    }

    client.close();
}

TEST(ConsumerTest, subscribeOrUnsubscribeOneTopic) {
    const std::string topic = "ConsumerSubOrUnsub" + std::to_string(time(nullptr));
    auto getPartition = [&topic](int partition) {
        return "persistent://public/default/" + topic + "-partition-" + std::to_string(partition);
    };
    const std::string subName = "SubscriptionName";

    // Create a topic with 3 partitions
    int res = makePutRequest(adminV2Url + "persistent/public/default/" + topic + "/partitions", "3");
    ASSERT_TRUE(res == 204 || res == 409) << "res = " << res;

    Client client(lookupUrl);

    ProducerConfiguration producerConfig;
    producerConfig.setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topic, producerConfig, producer));

    // 1. Subscribe partition-0 and partition-1
    ConsumerConfiguration consumerConfig;
    consumerConfig.setSubscriptionInitialPosition(InitialPositionEarliest);

    std::vector<std::string> topics{getPartition(0), getPartition(1)};
    Consumer consumer;
    ASSERT_EQ(ResultOk, client.subscribe(topics, subName, consumerConfig, consumer));

    for (int i = 0; i < 3 * 10; i++) {
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("x").build()));
    }

    std::set<std::string> topicSet;
    for (int i = 0; i < 2 * 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        topicSet.emplace(msg.getTopicName());
    }

    ASSERT_EQ(topicSet, (std::set<std::string>{getPartition(0), getPartition(1)}));

    // 2. Subscribe partition-2
    ASSERT_EQ(ResultOk, consumer.subscribeOneTopic(getPartition(2)));
    topicSet.clear();
    for (int i = 0; i < 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        topicSet.emplace(msg.getTopicName());
    }
    ASSERT_EQ(topicSet, (std::set<std::string>{getPartition(2)}));

    /*
    // 3. Unsubscribe partition-0
    std::cout << "[START] Unsub partition-0" << std::endl;
    ASSERT_EQ(ResultOk, consumer.unsubscribeOneTopic(getPartition(0)));
    std::cout << "Unsub partition-0" << std::endl;
    topicSet.clear();
    for (int i = 0; i < 3 * 10; i++) {
        ASSERT_EQ(ResultOk, producer.send(MessageBuilder().setContent("x").build()));
    }
    for (int i = 0; i < 2 * 10; i++) {
        Message msg;
        ASSERT_EQ(ResultOk, consumer.receive(msg, 3000));
        topicSet.emplace(msg.getTopicName());
    }
    ASSERT_EQ(topicSet, (std::set<std::string>{getPartition(1), getPartition(2)}));
    */

    client.close();
}
