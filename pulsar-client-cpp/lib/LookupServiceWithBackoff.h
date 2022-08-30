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
#pragma once

#include <memory>
#include "lib/Backoff.h"
#include "lib/DelayedTask.h"
#include "lib/LookupService.h"
#include "lib/SynchronizedHashMap.h"

namespace pulsar {

class LookupServiceWithBackoff : public LookupService,
                                 public std::enable_shared_from_this<LookupServiceWithBackoff> {
   public:
    template <typename... Args>
    static std::shared_ptr<LookupServiceWithBackoff> create(Args&&... args) {
        struct LookupServiceWithBackoffImpl : public LookupServiceWithBackoff {
            LookupServiceWithBackoffImpl(Args&&... args)
                : LookupServiceWithBackoff(std::forward<Args>(args)...) {}
        };
        return std::static_pointer_cast<LookupServiceWithBackoff>(
            std::make_shared<LookupServiceWithBackoffImpl>(std::forward<Args>(args)...));
    }

    LookupResultFuture getBroker(const TopicName& topicName) override {
        return executeAsync<LookupResult>("get-broker-" + topicName.toString(),
                                          [this, topicName] { return lookupService_->getBroker(topicName); });
    }

    Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const TopicNamePtr& topicName) override {
        return executeAsync<LookupDataResultPtr>(
            "get-partition-metadata-" + topicName->toString(),
            [this, topicName] { return lookupService_->getPartitionMetadataAsync(topicName); });
    }

    Future<Result, NamespaceTopicsPtr> getTopicsOfNamespaceAsync(const NamespaceNamePtr& nsName) override {
        return executeAsync<NamespaceTopicsPtr>(
            "get-topics-of-namespace-" + nsName->toString(),
            [this, nsName] { return lookupService_->getTopicsOfNamespaceAsync(nsName); });
    }

    template <typename T>
    Future<Result, T> executeAsync(const std::string& key, std::function<Future<Result, T>()> f) {
        Promise<Result, T> promise;
        auto timeout = std::make_shared<DelayedTask>(ioService_);
        std::weak_ptr<LookupServiceWithBackoff> weakSelf{shared_from_this()};
        timeout->execute(boost::posix_time::seconds(timeoutInSeconds_), [this, weakSelf, promise, key] {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            rescheduleTasks_.remove(key);
            promise.setFailed(ResultTimeout);
        });
        executeAsync(key, f, promise, timeout);
        return promise.getFuture();
    }

    size_t getNumberOfPendingRescheduleTasks() const noexcept { return rescheduleTasks_.size(); }

   private:
    std::unique_ptr<LookupService> lookupService_;
    const int timeoutInSeconds_;
    Backoff backoff_;
    boost::asio::io_service& ioService_;
    SynchronizedHashMap<std::string, std::unique_ptr<DelayedTask>> rescheduleTasks_;

    LookupServiceWithBackoff(std::unique_ptr<LookupService>&& lookupService, int timeoutInSeconds,
                             boost::asio::io_service& ioService)
        : lookupService_(std::move(lookupService)),
          timeoutInSeconds_(timeoutInSeconds),
          backoff_(timeoutInSeconds),
          ioService_(ioService) {}

    template <typename T>
    void executeAsync(const std::string& key, std::function<Future<Result, T>()> f,
                      Promise<Result, T> promise, std::shared_ptr<DelayedTask> timeout) {
        std::weak_ptr<LookupServiceWithBackoff> weakSelf{shared_from_this()};
        f().addListener([this, weakSelf, key, f, promise, timeout](Result result, const T& value) {
            auto self = weakSelf.lock();
            if (!self) {
                return;
            }
            if (promise.isComplete()) {
                // The associated future has been completed by timeout
                return;
            }
            if (result == ResultOk) {
                rescheduleTasks_.remove(key);
                promise.setValue(value);
            } else if (result != ResultRetryable && result != ResultConnectError &&
                       result != ResultNotConnected) {
                rescheduleTasks_.remove(key);
                promise.setFailed(result);
            } else {
                auto it =
                    rescheduleTasks_.emplace(key, std::unique_ptr<DelayedTask>(new DelayedTask(ioService_)))
                        .first;
                // TODO: handle the total timeout
                it->second->execute(backoff_.next(), [this, weakSelf, key, f, promise, timeout] {
                    auto self = weakSelf.lock();
                    if (!self) {
                        return;
                    }
                    executeAsync(key, f, promise, timeout);
                });
            }
        });
    }
};

}  // namespace pulsar
