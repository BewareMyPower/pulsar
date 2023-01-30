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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import java.io.IOException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.TopicMessageIdSerDes;
import org.testng.annotations.Test;

public class MessageIdSerializationTest {

    @Test
    public void testProtobufSerialization1() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, 3);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
        assertEquals(MessageId.fromByteArrayWithTopic(serializedId, "my-topic"), id);
    }

    @Test
    public void testProtobufSerialization2() throws Exception {
        MessageId id = new MessageIdImpl(1, 2, -1);
        byte[] serializedId = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serializedId), id);
        assertEquals(MessageId.fromByteArrayWithTopic(serializedId, "my-topic"), id);
    }

    @Test
    public void testBatchSizeNotSet() throws Exception {
        MessageId id = new BatchMessageIdImpl(1L, 2L, 3, 4, -1,
                BatchMessageAckerDisabled.INSTANCE);
        byte[] serialized = id.toByteArray();
        assertEquals(MessageId.fromByteArray(serialized), id);
        assertEquals(MessageId.fromByteArrayWithTopic(serialized, "my-topic"), id);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testProtobufSerializationNull() throws Exception {
        MessageId.fromByteArray(null);
    }

    @Test(expectedExceptions = IOException.class)
    void testProtobufSerializationEmpty() throws Exception {
        MessageId.fromByteArray(new byte[0]);
    }

    @Test
    public void testTopicMessageIdSerDes() throws Exception {
        String topic = "persistent://public/default/my-topic";
        MessageId id1 = new MessageIdImpl(1L, 2L, 3);
        MessageId id2 = new BatchMessageIdImpl(1L, 2L, 3, 4);

        TopicMessageId topicMessageId1 = convert(topic, id1);
        assertEquals(topicMessageId1.getOwnerTopic(), topic);
        assertEquals(topicMessageId1.toByteArray(), id1.toByteArray());

        TopicMessageId topicMessageId2 = convert(topic, id2);
        assertEquals(topicMessageId2.getOwnerTopic(), topic);
        assertEquals(topicMessageId2.toByteArray(), id2.toByteArray());
    }

    private static TopicMessageId convert(String topic, MessageId messageId) throws IOException {
        return TopicMessageIdSerDes.deserialize(
                TopicMessageIdSerDes.serialize(
                        TopicMessageId.create(topic, messageId)
                )
        );
    }
}
