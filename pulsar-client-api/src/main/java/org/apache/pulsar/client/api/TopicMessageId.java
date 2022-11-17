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
package org.apache.pulsar.client.api;

/**
 * A {@link MessageId} object that carries the topic name so that the MessageId can be used in acknowledgment.
 *
 * @see MessageAcknowledger
 */
public interface TopicMessageId extends MessageId {

    /**
     * Get the name of the topic that a message belongs to.
     *
     * @return the topic name
     * @implNote The returned topic name must be completed, e.g. "persistent://public/default/topic". For messages from
     *   a partitioned topic, this method should return the topic name of a specific partition, e.g.
     *   "persistent://public/default/topic-partition-0".
     */
    String getOwnerTopic();

    static TopicMessageId create(String topic, MessageId messageId) {
        return new TopicMessageId() {
            @Override
            public String getOwnerTopic() {
                return topic;
            }

            @Override
            public byte[] toByteArray() {
                return messageId.toByteArray();
            }

            @Override
            public int compareTo(MessageId o) {
                return messageId.compareTo(o);
            }
        };
    }
}
