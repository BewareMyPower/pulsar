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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * To keep the backward compatibility, {@link TopicMessageId#toByteArray()} should not serialize the owner topic. This
 * class provides a convenient way for users to serialize a TopicMessageId with its owner topic serialized.
 * <p>
 * The format is:
 * 1. 4 bytes that represent the length of the serialized topic name.
 * 2. N bytes that represent the UTF-8 serialized topic name.
 * 3. Serialized bytes from {@link MessageId#toByteArray()}.
 */
public class TopicMessageIdSerDes {

    public static byte[] serialize(TopicMessageId topicMessageId) {
        final byte[] topicBytes = topicMessageId.getOwnerTopic().getBytes(StandardCharsets.UTF_8);
        final byte[] messageIdBytes = topicMessageId.toByteArray();

        int topicLength = topicBytes.length;
        final byte[] serialized = new byte[4 + topicLength + messageIdBytes.length];
        serialized[0] = (byte) (topicLength >>> 24);
        serialized[1] = (byte) (topicLength >>> 16);
        serialized[2] = (byte) (topicLength >>> 8);
        serialized[3] = (byte) topicLength;

        System.arraycopy(topicBytes, 0, serialized, 4, topicLength);
        System.arraycopy(messageIdBytes, 0, serialized, 4 + topicLength, messageIdBytes.length);
        return serialized;
    }

    public static TopicMessageId deserialize(byte[] bytes) throws IOException {
        if (bytes.length < 4) {
            throw new IOException("No length field");
        }
        int topicLength = 0;
        for (int i = 0; i < 4; i++) {
            topicLength <<= 8;
            topicLength |= bytes[i] & 0xFF;
        }
        if (bytes.length < 4 + topicLength) {
            throw new IOException("Read topic length " + topicLength + ", while there is only " + (bytes.length - 4)
                    + " bytes remained");
        }

        final byte[] topicBytes = new byte[topicLength];
        System.arraycopy(bytes, 4, topicBytes, 0, topicLength);
        final String topic = new String(topicBytes, StandardCharsets.UTF_8);

        final byte[] messageIdBytes = new byte[bytes.length - 4 - topicLength];
        System.arraycopy(bytes, 4 + topicLength, messageIdBytes, 0, messageIdBytes.length);
        final MessageId messageId = MessageId.fromByteArray(messageIdBytes);

        return TopicMessageId.create(topic, messageId);
    }
}
