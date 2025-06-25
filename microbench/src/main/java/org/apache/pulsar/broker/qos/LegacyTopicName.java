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
package org.apache.pulsar.broker.qos;

import com.google.common.base.Splitter;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.util.Codec;

public class LegacyTopicName {

    public static final String PUBLIC_TENANT = "public";
    public static final String DEFAULT_NAMESPACE = "default";

    public static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";

    private final String completeTopicName;

    private final TopicDomain domain;
    private final String tenant;
    private final String cluster;
    private final String namespacePortion;
    private final String localName;

    private final NamespaceName namespaceName;

    private final int partitionIndex;

    public LegacyTopicName(String completeTopicName) {
        try {
            // The topic name can be in two different forms, one is fully qualified topic name,
            // the other one is short topic name
            if (!completeTopicName.contains("://")) {
                // The short topic name can be:
                // - <topic>
                // - <property>/<namespace>/<topic>
                String[] parts = StringUtils.split(completeTopicName, '/');
                if (parts.length == 3) {
                    completeTopicName = TopicDomain.persistent.name() + "://" + completeTopicName;
                } else if (parts.length == 1) {
                    completeTopicName = TopicDomain.persistent.name() + "://"
                            + PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE + "/" + parts[0];
                } else {
                    throw new IllegalArgumentException(
                            "Invalid short topic name '" + completeTopicName + "', it should be in the format of "
                                    + "<tenant>/<namespace>/<topic> or <topic>");
                }
            }

            // The fully qualified topic name can be in two different forms:
            // new:    persistent://tenant/namespace/topic
            // legacy: persistent://tenant/cluster/namespace/topic

            List<String> parts = Splitter.on("://").limit(2).splitToList(completeTopicName);
            this.domain = TopicDomain.getEnum(parts.get(0));

            String rest = parts.get(1);

            // The rest of the name can be in different forms:
            // new:    tenant/namespace/<localName>
            // legacy: tenant/cluster/namespace/<localName>
            // Examples of localName:
            // 1. some, name, xyz
            // 2. xyz-123, feeder-2


            parts = Splitter.on("/").limit(4).splitToList(rest);
            if (parts.size() == 3) {
                // New topic name without cluster name
                this.tenant = parts.get(0);
                this.cluster = null;
                this.namespacePortion = parts.get(1);
                this.localName = parts.get(2);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(tenant, namespacePortion);
            } else if (parts.size() == 4) {
                // Legacy topic name that includes cluster name
                this.tenant = parts.get(0);
                this.cluster = parts.get(1);
                this.namespacePortion = parts.get(2);
                this.localName = parts.get(3);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(tenant, cluster, namespacePortion);
            } else {
                throw new IllegalArgumentException("Invalid topic name: " + completeTopicName);
            }

            if (StringUtils.isBlank(localName)) {
                throw new IllegalArgumentException(String.format("Invalid topic name: %s. Topic local name must not"
                        + " be blank.", completeTopicName));
            }

        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid topic name: " + completeTopicName, e);
        }
        if (isV2()) {
            this.completeTopicName = String.format("%s://%s/%s/%s",
                    domain, tenant, namespacePortion, localName);
        } else {
            this.completeTopicName = String.format("%s://%s/%s/%s/%s",
                    domain, tenant, cluster,
                    namespacePortion, localName);
        }
    }

    public static int getPartitionIndex(String topic) {
        int partitionIndex = -1;
        if (topic.contains(PARTITIONED_TOPIC_SUFFIX)) {
            try {
                String idx = StringUtils.substringAfterLast(topic, PARTITIONED_TOPIC_SUFFIX);
                partitionIndex = Integer.parseInt(idx);
                if (partitionIndex < 0) {
                    // for the "topic-partition--1"
                    partitionIndex = -1;
                } else if (StringUtils.length(idx) != String.valueOf(partitionIndex).length()) {
                    // for the "topic-partition-01"
                    partitionIndex = -1;
                }
            } catch (NumberFormatException nfe) {
                // ignore exception
            }
        }

        return partitionIndex;
    }

    /**
     * get topic full name from managedLedgerName.
     *
     * @return the topic full name, format -> domain://tenant/namespace/topic
     */
    public static String fromPersistenceNamingEncoding(String mlName) {
        // The managedLedgerName convention is: tenant/namespace/domain/topic
        // We want to transform to topic full name in the order: domain://tenant/namespace/topic
        if (mlName == null || mlName.length() == 0) {
            return mlName;
        }
        List<String> parts = Splitter.on("/").splitToList(mlName);
        String tenant;
        String cluster;
        String namespacePortion;
        String domain;
        String localName;
        if (parts.size() == 4) {
            tenant = parts.get(0);
            namespacePortion = parts.get(1);
            domain = parts.get(2);
            localName = Codec.decode(parts.get(3));
            return String.format("%s://%s/%s/%s", domain, tenant, namespacePortion, localName);
        } else if (parts.size() == 5) {
            tenant = parts.get(0);
            cluster = parts.get(1);
            namespacePortion = parts.get(2);
            domain = parts.get(3);
            localName = Codec.decode(parts.get(4));
            return String.format("%s://%s/%s/%s/%s", domain, tenant, cluster, namespacePortion, localName);
        } else {
            throw new IllegalArgumentException("Invalid managedLedger name: " + mlName);
        }
    }


    public boolean isV2() {
        return cluster == null;
    }
}
