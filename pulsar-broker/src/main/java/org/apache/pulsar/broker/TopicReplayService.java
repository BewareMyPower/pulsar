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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The metadata of message deduplication or transaction metadata is usually stored by two parts:
 * 1. A snapshot stored in an external service (e.g. metadata store or a system topic), which represents the metadata
 *    until the position.
 * 2. The entries started from that position.
 * To get the 2nd part, a managed cursor is required for replaying entries of the topic from the position.
 * For example, given a topic with 1 ledger and 10 entries.
 * - Task 1: snapshot position is (0, 1), create a cursor to read entry range [1, 10) and process these 9 entries.
 * - Task 2: snapshot position is (0, 5), create a cursor to read entry range [5, 10) and process these 5 entries.
 * There will be two managed cursors created to read entries separately, which is a waste or resource.
 * This class is used to avoid the waste of resource with the following steps:
 * 1. Collect these two positions and start topic replay once these positions are available.
 * 2. Create one cursor to read entry from the earliest position (0, 1).
 * 3. Read entry range [1, 5) and process them for task 1.
 * 4. Read entry range [5, 10) and process them for task 1 and task 2.
 */
// TODO: move it to managed-ledger module
@RequiredArgsConstructor
@Slf4j
public class TopicReplayService {

    private final Map<String, ReplayTask> replayTasks = new ConcurrentHashMap<>();
    private final ManagedLedger managedLedger;

    /**
     * Register a replay task.
     *
     * @param name the task name
     * @param task the replay task
     */
    public void register(String name, ReplayTask task) {
        if (replayTasks.computeIfAbsent(name, __ -> task) == task) {
            log.info("Registered replay task for {}", name);
        } else {
            log.error("Failed to register replay task because {} already exists", name);
        }
    }

    /**
     * Perform the topic replay for all tasks.
     *
     * @param topicName the topic to replay
     * @param executor the executor to process the entry
     * @return the future of the last position replayed
     */
    public CompletableFuture<Position> replay(TopicName topicName, Executor executor) {
        final var positionFutures = replayTasks.values().stream().map(ReplayTask::getPosition).toList();
        if (positionFutures.isEmpty()) {
            // TODO: should we fail it?
            return CompletableFuture.completedFuture(managedLedger.getLastConfirmedEntry());
        }
        return FutureUtil.waitForAll(positionFutures).thenCompose(__ -> {
            final var positions = positionFutures.stream().map(CompletableFuture::join).toList();
            var earliestPosition = positions.get(0);
            for (int i = 1; i < positions.size(); i++) {
                final var position = positions.get(i);
                if (position.compareTo(earliestPosition) < 0) {
                    earliestPosition = position;
                }
            }
            try {
                final var cursor = managedLedger.newNonDurableCursor(earliestPosition, topicName + "-replay");
                return replayCursor(cursor, earliestPosition);
            } catch (ManagedLedgerException e) {
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    private CompletableFuture<Position> replayCursor(ManagedCursor cursor, Position currentPosition) {
        if (!cursor.hasMoreEntries()) {
            return CompletableFuture.completedFuture(currentPosition);
        }
        return readEntries(cursor).thenCompose(entries -> {
            if (entries.isEmpty()) {
                return replayCursor(cursor, currentPosition);
            }
            Position lastPosition = currentPosition;
            for (final var entry : entries) {
                try {
                    final var position = entry.getPosition();
                    for (final var keyValue : replayTasks.entrySet()) {
                        final var name = keyValue.getKey();
                        final var task = keyValue.getValue();
                        try {
                            if (position.compareTo(task.getPosition().join()) >= 0) {
                                task.processEntry(entry);
                            }
                        } catch (Throwable throwable) {
                            log.error("Failed to process entry {} for task {}", position, name, throwable);
                        }
                    }
                    // TODO: support cancelling the topic replay
                    lastPosition = position;
                } finally {
                    entry.release();
                }
            }
            return replayCursor(cursor, lastPosition);
        });
    }

    private static CompletableFuture<List<Entry>> readEntries(ManagedCursor cursor) {
        final var future = new CompletableFuture<List<Entry>>();
        cursor.asyncReadEntries(100, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);
        return future;
    }

    public interface ReplayTask {

        /**
         * @return the future of the position to start replaying (inclusive)
         */
        CompletableFuture<Position> getPosition();

        /**
         * Process the entry.
         * {@link Entry#release()} will be called after this method is called, so the implementation should not call
         * `release()` manually or store the entry somewhere else. Any exception thrown by this method will be caught.
         */
        void processEntry(Entry entry);
    }
}
