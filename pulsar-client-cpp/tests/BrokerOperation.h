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
#include <assert.h>
#include <stdlib.h>
#include <string>

namespace pulsar {

// See ../test-conf/broker-<id>.conf for the broker id

inline int startBroker(int brokerId) {
    assert(brokerId >= 0 && brokerId <= 1);
    return system(("bash ../broker-ops.sh start " + std::to_string(brokerId)).c_str());
}

inline int stopBroker(int brokerId) {
    assert(brokerId >= 0 && brokerId <= 1);
    return system(("bash ../broker-ops.sh stop " + std::to_string(brokerId)).c_str());
}

}  // namespace pulsar
