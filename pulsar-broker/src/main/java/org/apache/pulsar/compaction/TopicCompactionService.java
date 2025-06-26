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
package org.apache.pulsar.compaction;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.jspecify.annotations.NonNull;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TopicCompactionService extends AutoCloseable {
    /**
     * Compact the topic.
     * Topic Compaction is a key-based retention mechanism. It keeps the most recent value for a given key and
     * user reads compacted data from TopicCompactionService.
     *
     * @return a future that will be completed when the compaction is done.
     */
    CompletableFuture<Void> compact();

    /**
     * Read the compacted entries from the TopicCompactionService.
     *
     * @param startPosition         the position to start reading from.
     * @param numberOfEntriesToRead the maximum number of entries to read.
     * @return a future that will be completed with the list of entries, this list can be null.
     */
    CompletableFuture<List<Entry>> readCompactedEntries(@NonNull Position startPosition, int numberOfEntriesToRead);

    /**
     * Get the last compacted position from the TopicCompactionService.
     * <p>
     * The position is the mark-delete position of the original topic when the last compaction was performed.
     * </p>
     * @return a future that will be completed with the last compacted position, this position can be null.
     */
    CompletableFuture<Position> getLastCompactedPosition();

    /**
    * Find the first entry's position that is greater or equal to target publishTime.
    *
    * @param publishTime  the publish time of entry.
    * @return the first entry's position that is greater or equal to target publishTime or
    *   {@link org.apache.bookkeeper.mledger.PositionFactory#EARLIEST} if the entry does not exist
    */
    CompletableFuture<Position> findPositionByPublishTime(long publishTime);

    /**
     * Get the last message's position of the original topic.
     * {@link MessagePosition#EARLIEST} will be returned if the last message does not exist.
     */
    CompletableFuture<MessagePosition> getLastMessagePosition();

    /**
     * The position of a message.
     * It adds a new field to {@link Position} that represents the batch index of the message in the entry. The batch
     * index is -1 for a non-batched message or a non-positive integer when the entry is a batched message.
     */
    record MessagePosition(long ledgerId, long entryId, int batchIndex, long publishTime) {

        public static final MessagePosition EARLIEST = new MessagePosition(-1L, -1L, -1, 0L);
        public static final CompletableFuture<MessagePosition> EARLIEST_FUTURE =
                CompletableFuture.completedFuture(EARLIEST);
    }
}
