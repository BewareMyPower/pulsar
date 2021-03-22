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
#ifndef ATOMICHELPER_H_
#define ATOMICHELPER_H_

#include <atomic>

namespace pulsar {

template <typename T, typename UnaryOp>
inline T getAndUpdate(std::atomic<T>& x, UnaryOp op) {
    while (true) {
        T oldValue = x.load();
        const T newValue = op(oldValue);
        if (x.compare_exchange_weak(oldValue, newValue)) {
            return oldValue;
        }
    }
}

}  // namespace pulsar

#endif /* ATOMICHELPER_H_ */
