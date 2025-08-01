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
package org.apache.pulsar.broker;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.jspecify.annotations.NonNull;

/**
 * Multiple brokers with a real test Zookeeper server (instead of the mock server).
 */
@Slf4j
public abstract class MultiBrokerTestZKBaseTest extends MultiBrokerBaseTest {
    TestZKServer testZKServer;
    List<MetadataStoreExtended> storesToClose = new ArrayList<>();

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        testZKServer = new TestZKServer();
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
        for (MetadataStoreExtended store : storesToClose) {
            try {
                store.close();
            } catch (Exception e) {
                log.error("Error in closing metadata store", e);
            }
        }
        storesToClose.clear();
        if (testZKServer != null) {
            try {
                testZKServer.close();
            } catch (Exception e) {
                log.error("Error in stopping ZK server", e);
            }
            testZKServer = null;
        }
    }

    @Override
    protected PulsarTestContext.Builder createPulsarTestContextBuilder(ServiceConfiguration conf) {
        return super.createPulsarTestContextBuilder(conf)
                .spyNoneByDefault()
                .localMetadataStore(createMetadataStore(MetadataStoreConfig.METADATA_STORE))
                .configurationMetadataStore(createMetadataStore(MetadataStoreConfig.CONFIGURATION_METADATA_STORE));
    }

    @NonNull
    protected MetadataStoreExtended createMetadataStore(String metadataStoreName)  {
        try {
            MetadataStoreExtended store =
                    MetadataStoreExtended.create(testZKServer.getConnectionString(),
                            MetadataStoreConfig.builder().metadataStoreName(metadataStoreName).build());
            storesToClose.add(store);
            return store;
        } catch (MetadataStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
