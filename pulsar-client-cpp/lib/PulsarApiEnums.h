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

// Since enum cannot be forward declared, this header includes the enums with the same name and definitions in
// PulsarApi.pb.h. Use constants to avoid conversion between two enums.

namespace pulsar {

using CommandSubscribe_SubType = int;
constexpr int CommandSubscribe_SubType_Exclusive = 0;
constexpr int CommandSubscribe_SubType_Shared = 1;
constexpr int CommandSubscribe_SubType_Failover = 2;
constexpr int CommandSubscribe_SubType_Key_Shared = 3;

using CommandSubscribe_InitialPosition = int;
constexpr int CommandSubscribe_InitialPosition_Latest = 0;
constexpr int CommandSubscribe_InitialPosition_Earliest = 1;

using CommandAck_AckType = int;
constexpr int CommandAck_AckType_Individual = 0;
constexpr int CommandAck_AckType_Cumulative = 1;

using CommandAck_ValidationError = int;
constexpr int CommandAck_ValidationError_UncompressedSizeCorruption = 0;
constexpr int CommandAck_ValidationError_DecompressionError = 1;
constexpr int CommandAck_ValidationError_ChecksumMismatch = 2;
constexpr int CommandAck_ValidationError_BatchDeSerializeError = 3;
constexpr int CommandAck_ValidationError_DecryptionError = 4;

using BaseCommand_Type = int;
constexpr int BaseCommand_Type_CONNECT = 2;
constexpr int BaseCommand_Type_CONNECTED = 3;
constexpr int BaseCommand_Type_SUBSCRIBE = 4;
constexpr int BaseCommand_Type_PRODUCER = 5;
constexpr int BaseCommand_Type_SEND = 6;
constexpr int BaseCommand_Type_SEND_RECEIPT = 7;
constexpr int BaseCommand_Type_SEND_ERROR = 8;
constexpr int BaseCommand_Type_MESSAGE = 9;
constexpr int BaseCommand_Type_ACK = 10;
constexpr int BaseCommand_Type_FLOW = 11;
constexpr int BaseCommand_Type_UNSUBSCRIBE = 12;
constexpr int BaseCommand_Type_SUCCESS = 13;
constexpr int BaseCommand_Type_ERROR = 14;
constexpr int BaseCommand_Type_CLOSE_PRODUCER = 15;
constexpr int BaseCommand_Type_CLOSE_CONSUMER = 16;
constexpr int BaseCommand_Type_PRODUCER_SUCCESS = 17;
constexpr int BaseCommand_Type_PING = 18;
constexpr int BaseCommand_Type_PONG = 19;
constexpr int BaseCommand_Type_REDELIVER_UNACKNOWLEDGED_MESSAGES = 20;
constexpr int BaseCommand_Type_PARTITIONED_METADATA = 21;
constexpr int BaseCommand_Type_PARTITIONED_METADATA_RESPONSE = 22;
constexpr int BaseCommand_Type_LOOKUP = 23;
constexpr int BaseCommand_Type_LOOKUP_RESPONSE = 24;
constexpr int BaseCommand_Type_CONSUMER_STATS = 25;
constexpr int BaseCommand_Type_CONSUMER_STATS_RESPONSE = 26;
constexpr int BaseCommand_Type_REACHED_END_OF_TOPIC = 27;
constexpr int BaseCommand_Type_SEEK = 28;
constexpr int BaseCommand_Type_GET_LAST_MESSAGE_ID = 29;
constexpr int BaseCommand_Type_GET_LAST_MESSAGE_ID_RESPONSE = 30;
constexpr int BaseCommand_Type_ACTIVE_CONSUMER_CHANGE = 31;
constexpr int BaseCommand_Type_GET_TOPICS_OF_NAMESPACE = 32;
constexpr int BaseCommand_Type_GET_TOPICS_OF_NAMESPACE_RESPONSE = 33;
constexpr int BaseCommand_Type_GET_SCHEMA = 34;
constexpr int BaseCommand_Type_GET_SCHEMA_RESPONSE = 35;
constexpr int BaseCommand_Type_AUTH_CHALLENGE = 36;
constexpr int BaseCommand_Type_AUTH_RESPONSE = 37;
constexpr int BaseCommand_Type_ACK_RESPONSE = 38;
constexpr int BaseCommand_Type_GET_OR_CREATE_SCHEMA = 39;
constexpr int BaseCommand_Type_GET_OR_CREATE_SCHEMA_RESPONSE = 40;
constexpr int BaseCommand_Type_NEW_TXN = 50;
constexpr int BaseCommand_Type_NEW_TXN_RESPONSE = 51;
constexpr int BaseCommand_Type_ADD_PARTITION_TO_TXN = 52;
constexpr int BaseCommand_Type_ADD_PARTITION_TO_TXN_RESPONSE = 53;
constexpr int BaseCommand_Type_ADD_SUBSCRIPTION_TO_TXN = 54;
constexpr int BaseCommand_Type_ADD_SUBSCRIPTION_TO_TXN_RESPONSE = 55;
constexpr int BaseCommand_Type_END_TXN = 56;
constexpr int BaseCommand_Type_END_TXN_RESPONSE = 57;
constexpr int BaseCommand_Type_END_TXN_ON_PARTITION = 58;
constexpr int BaseCommand_Type_END_TXN_ON_PARTITION_RESPONSE = 59;
constexpr int BaseCommand_Type_END_TXN_ON_SUBSCRIPTION = 60;
constexpr int BaseCommand_Type_END_TXN_ON_SUBSCRIPTION_RESPONSE = 61;
constexpr int BaseCommand_Type_TC_CLIENT_CONNECT_REQUEST = 62;
constexpr int BaseCommand_Type_TC_CLIENT_CONNECT_RESPONSE = 63;
constexpr int BaseCommand_Type_WATCH_TOPIC_LIST = 64;
constexpr int BaseCommand_Type_WATCH_TOPIC_LIST_SUCCESS = 65;
constexpr int BaseCommand_Type_WATCH_TOPIC_UPDATE = 66;
constexpr int BaseCommand_Type_WATCH_TOPIC_LIST_CLOSE = 67;

}  // namespace pulsar
