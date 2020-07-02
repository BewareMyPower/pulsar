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
#include <gtest/gtest.h>
#include <pulsar/Client.h>

#include <set>
#include <chrono>
#include <thread>
#include <memory>

#include "HttpHelper.h"

using namespace pulsar;

static const std::string serviceUrl = "pulsar://localhost:6650";
static const std::string adminUrl = "http://localhost:8080/";

inline std::string createTopicOperateUrl(const std::string& topicName) {
    return adminUrl + "admin/v2/persistent/public/default/" + topicName + "/partitions";
}

static ClientConfiguration newClientConfig(bool enablePartitionsUpdate) {
    ClientConfiguration clientConfig;
    if (enablePartitionsUpdate) {
        clientConfig.setPartititionsUpdateInterval(1);  // 1s
    } else {
        clientConfig.setPartititionsUpdateInterval(0);  // disable
    }
    return clientConfig;
}

// In round robin routing mode, if N messages were sent to a topic with N partitions, each partition must have
// received 1 message. So we check whether producer/consumer have increased along with partitions by checking
// partitions' count of N messages.
// Use std::set because it doesn't allow repeated elements.
class PartitionsSet {
   public:
    PartitionsSet(const std::string& topicName) : topicName_(topicName) {}

    size_t size() const { return names_.size(); }

    Result initProducer(bool enablePartitionsUpdate) {
        clientForProducer_.reset(new Client(serviceUrl, newClientConfig(enablePartitionsUpdate)));
        const auto producerConfig =
            ProducerConfiguration().setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
        return clientForProducer_->createProducer(topicName_, producerConfig, producer_);
    }

    Result initConsumer(bool enablePartitionsUpdate) {
        clientForConsumer_.reset(new Client(serviceUrl, newClientConfig(enablePartitionsUpdate)));
        return clientForConsumer_->subscribe(topicName_, "SubscriptionName", consumer_);
    }

    void close() {
        producer_.close();
        clientForProducer_->close();
        consumer_.close();
        clientForConsumer_->close();
    }

    void doSendAndReceive(int numMessagesSend, int numMessagesReceive) {
        names_.clear();
        for (int i = 0; i < numMessagesSend; i++) {
            producer_.send(MessageBuilder().setContent("a").build());
        }
        while (numMessagesReceive > 0) {
            Message msg;
            if (consumer_.receive(msg, 100) == ResultOk) {
                names_.emplace(msg.getTopicName());
                consumer_.acknowledge(msg);
                numMessagesReceive--;
            }
        }
    }

   private:
    std::string topicName_;

    std::set<std::string> names_;

    std::unique_ptr<Client> clientForProducer_;
    Producer producer_;

    std::unique_ptr<Client> clientForConsumer_;
    Consumer consumer_;
};

static void waitForPartitionsUpdated() {
    // Assume producer and consumer have updated partitions in 3 seconds if enabled
    std::this_thread::sleep_for(std::chrono::seconds(3));
}

TEST(PartitionsUpdateTest, testConfigPartitionsUpdateInterval) {
    ClientConfiguration clientConfig;
    ASSERT_EQ(60, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(0);
    ASSERT_EQ(0, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(1);
    ASSERT_EQ(1, clientConfig.getPartitionsUpdateInterval());

    clientConfig.setPartititionsUpdateInterval(-1);
    ASSERT_EQ(static_cast<unsigned int>(-1), clientConfig.getPartitionsUpdateInterval());
}

TEST(PartitionsUpdateTest, testPartitionsUpdate) {
    const std::string topicName = "partitions-update-test-partitions-update";
    const std::string topicOperateUrl = createTopicOperateUrl(topicName);

    // Ensure `topicName` doesn't exist before created
    makeDeleteRequest(topicOperateUrl);
    // Create a 2 partitions topic
    int res = makePutRequest(topicOperateUrl, "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    PartitionsSet partitionsSet(topicName);

    // 1. Both producer and consumer enable partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(true));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(true));

    res = makePostRequest(topicOperateUrl, "3");  // update partitions to 3
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(3, 3);
    ASSERT_EQ(3, partitionsSet.size());
    partitionsSet.close();

    // 2. Only producer enables partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(true));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(false));

    res = makePostRequest(topicOperateUrl, "5");  // update partitions to 5
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(5, 3);  // can't consume partition-3,4
    ASSERT_EQ(3, partitionsSet.size());
    partitionsSet.close();

    // 3. Only consumer enables partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(false));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(true));

    res = makePostRequest(topicOperateUrl, "7");  // update partitions to 7
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(7, 7);
    ASSERT_EQ(5, partitionsSet.size());
    partitionsSet.close();

    // 4. Both producer and consumer disables partitions update
    ASSERT_EQ(ResultOk, partitionsSet.initProducer(false));
    ASSERT_EQ(ResultOk, partitionsSet.initConsumer(false));

    res = makePostRequest(topicOperateUrl, "10");  // update partitions to 10
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;
    waitForPartitionsUpdated();

    partitionsSet.doSendAndReceive(10, 10);
    ASSERT_EQ(7, partitionsSet.size());
    partitionsSet.close();
}

TEST(PartitionsUpdateTest, testMultiFailoverConsumers) {
    const std::string topicName = "partitions-update-test-multi-failover-consumers";
    const std::string topicOperateUrl = createTopicOperateUrl(topicName);
    const std::string subscriptionName = "SubscriptionName";

    makeDeleteRequest(topicOperateUrl);

    // Create a 1 partition topic
    int res = makePutRequest(topicOperateUrl, "1");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    Client client(serviceUrl, newClientConfig(true));
    const auto producerConfig =
        ProducerConfiguration().setPartitionsRoutingMode(ProducerConfiguration::RoundRobinDistribution);
    const auto consumerConfig = ConsumerConfiguration().setConsumerType(ConsumerFailover);

    Consumer consumers[3];
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subscriptionName, consumerConfig, consumers[0]));

    Producer producer;
    ASSERT_EQ(ResultOk, client.createProducer(topicName, producerConfig, producer));

    auto sendInt = [&producer](int i) -> Result {
        Message msg = MessageBuilder().setContent(std::to_string(i)).build();
        return producer.send(msg);
    };

    // Increase partitions to 2
    res = makePostRequest(topicOperateUrl, "2");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    waitForPartitionsUpdated();

    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(ResultOk, sendInt(i));
    }

    Message msg;
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(ResultOk, consumers[0].receive(msg, 3000));
        ASSERT_EQ(ResultOk, consumers[0].acknowledge(msg));
    }

    // Add new consumers to the same subscription
    //   Before: { partitions: 2, consumers: 1 }
    //   After:  { partitions: 2, consumers: 3 }
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subscriptionName, consumerConfig, consumers[1]));
    ASSERT_EQ(ResultOk, client.subscribe(topicName, subscriptionName, consumerConfig, consumers[2]));

    std::set<int32_t> partitionSetList[3];

    // `consumers[consumerIndex]` would receive as many messages as possible until timeout
    // The message id would be inserted to `partitionSetList[consumerIndex]`
    auto receiveLoop = [&consumers, &partitionSetList](int consumerIndex) {
        std::string name = "Consumer-" + std::to_string(consumerIndex);
        while (true) {
            Message msg;
            Result result = consumers[consumerIndex].receive(msg, 3000);  // 3 seconds timeout
            if (result == ResultTimeout) break;

            ASSERT_EQ(result, ResultOk);
            partitionSetList[consumerIndex].emplace(msg.getMessageId().partition());
        }
    };

    std::thread threads[3];
    for (int i = 0; i < 3; i++) {
        threads[i] = std::thread(receiveLoop, i);
    }
    for (int i = 0; i < 30; i++) {
        ASSERT_EQ(ResultOk, sendInt(i));
    }
    for (auto& t : threads) {
        t.join();
    }
    // Each consumer should receive messages from only one partition, except `consumers[2]`
    ASSERT_EQ(partitionSetList[0].size(), 1);
    ASSERT_EQ(partitionSetList[1].size(), 1);
    ASSERT_EQ(partitionSetList[2].size(), 0);

    // Increase partitions to 3
    //   Before: { partitions: 2, consumers: 3 }
    //   After:  { partitions: 3, consumers: 3 }
    res = makePostRequest(topicOperateUrl, "3");
    ASSERT_TRUE(res == 204 || res == 409) << "res: " << res;

    waitForPartitionsUpdated();

    // Now `consumers[2]` would be assigned a partition
    for (int i = 0; i < 3; i++) {
        threads[i] = std::thread(receiveLoop, i);
    }
    for (int i = 0; i < 30; i++) {
        ASSERT_EQ(ResultOk, sendInt(i));
    }
    for (auto& t : threads) {
        t.join();
    }
    // Each consumer should receive messages from only one partition
    ASSERT_EQ(partitionSetList[0].size(), 1);
    ASSERT_EQ(partitionSetList[1].size(), 1);
    ASSERT_EQ(partitionSetList[2].size(), 1);

    client.close();
}
