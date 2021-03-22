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
#include <lib/AtomicHelper.h>

using namespace pulsar;

TEST(AtomicHelperTest, testGetAndUpdate) {
    std::atomic_int x{10};
    auto func = [](int x) { return (x % 2 == 0) ? (x / 2) : (x + 1); };
    {
        const auto previous = getAndUpdate(x, func);
        ASSERT_EQ(previous, 10);
        ASSERT_EQ(x.load(), 5);
    }
    {
        const auto previous = getAndUpdate(x, func);
        ASSERT_EQ(previous, 5);
        ASSERT_EQ(x.load(), 6);
    }
}
