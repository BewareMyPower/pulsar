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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.compaction.CompactedTopicImpl.COMPACT_LEDGER_EMPTY;
import static org.apache.pulsar.compaction.CompactedTopicImpl.NEWER_THAN_COMPACTED;
import static org.apache.pulsar.compaction.CompactedTopicImpl.findStartPoint;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.jspecify.annotations.NonNull;


public class PulsarTopicCompactionService implements TopicCompactionService {

    private final String topic;

    private final CompactedTopicImpl compactedTopic;

    private final Supplier<Compactor> compactorSupplier;

    public PulsarTopicCompactionService(String topic, BookKeeper bookKeeper,
                                        Supplier<Compactor> compactorSupplier) {
        this.topic = topic;
        this.compactedTopic = new CompactedTopicImpl(bookKeeper);
        this.compactorSupplier = compactorSupplier;
    }

    @Override
    public CompletableFuture<Void> compact() {
        Compactor compactor;
        try {
            compactor = compactorSupplier.get();
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
        return compactor.compact(topic).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<List<Entry>> readCompactedEntries(@NonNull Position startPosition,
                                                               int numberOfEntriesToRead) {
        Objects.requireNonNull(startPosition);
        checkArgument(numberOfEntriesToRead > 0);

        CompletableFuture<List<Entry>> resultFuture = new CompletableFuture<>();

        Objects.requireNonNull(compactedTopic.getCompactedTopicContextFuture()).thenCompose(
                (context) -> findStartPoint(startPosition, context.ledger.getLastAddConfirmed(),
                        context.cache).thenCompose((startPoint) -> {
                    if (startPoint == COMPACT_LEDGER_EMPTY || startPoint == NEWER_THAN_COMPACTED) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }
                    long endPoint =
                            Math.min(context.ledger.getLastAddConfirmed(), startPoint + (numberOfEntriesToRead - 1));
                    return CompactedTopicImpl.readEntries(context.ledger, startPoint, endPoint);
                })).whenComplete((result, ex) -> {
                    if (ex == null) {
                        resultFuture.complete(result);
                    } else {
                        ex = FutureUtil.unwrapCompletionException(ex);
                        if (ex instanceof NoSuchElementException) {
                            resultFuture.complete(Collections.emptyList());
                        } else {
                            resultFuture.completeExceptionally(ex);
                        }
                    }
                });

        return resultFuture;
    }

    private CompletableFuture<Entry> readLastCompactedEntry() {
        return compactedTopic.readLastEntryOfCompactedLedger();
    }

    @Override
    public CompletableFuture<Position> getLastCompactedPosition() {
        return CompletableFuture.completedFuture(compactedTopic.getCompactionHorizon().orElse(null));
    }

    @Override
    public CompletableFuture<Position> findPositionByPublishTime(long publishTime) {
        return compactedTopic.findFirstMatchEntry(entry ->
                Commands.parseMessageMetadata(entry.getDataBuffer()).getPublishTime() > publishTime
        ).thenApply(entry -> entry != null ? entry.getPosition() : PositionFactory.EARLIEST);
    }

    public CompactedTopicImpl getCompactedTopic() {
        return compactedTopic;
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public CompletableFuture<MessagePosition> getLastMessagePosition() {
        return readLastCompactedEntry().thenApply(entry -> {
            if (entry == null) {
                return MessagePosition.EARLIEST;
            }
            try {
                // in this case, all the data has been compacted, so return the last position
                // in the compacted ledger to the client
                ByteBuf payload = entry.getDataBuffer();
                MessageMetadata metadata = Commands.parseMessageMetadata(payload);
                try {
                    final var batchIndex = calculateTheLastBatchIndexInBatch(metadata, payload);
                    final var publishTime = metadata.getPublishTime();
                    return new MessagePosition(entry.getLedgerId(), entry.getEntryId(), batchIndex, publishTime);
                } catch (IOException e) {
                    throw new CompletionException(new IOException("Failed to deserialize batched message from "
                            + "the last entry of the compacted ledger: " + e.getMessage()));
                }
            } finally {
                entry.release();
            }
        });
    }

    private static int calculateTheLastBatchIndexInBatch(MessageMetadata metadata, ByteBuf payload) throws IOException {
        int batchSize = metadata.getNumMessagesInBatch();
        if (batchSize <= 1){
            return -1;
        }
        if (metadata.hasCompression()) {
            var tmp = payload;
            CompressionType compressionType = metadata.getCompression();
            CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
            int uncompressedSize = metadata.getUncompressedSize();
            payload = codec.decode(payload, uncompressedSize);
            tmp.release();
        }
        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        int lastBatchIndexInBatch = -1;
        for (int i = 0; i < batchSize; i++){
            ByteBuf singleMessagePayload =
                    Commands.deSerializeSingleMessageInBatch(payload, singleMessageMetadata, i, batchSize);
            singleMessagePayload.release();
            if (singleMessageMetadata.isCompactedOut()){
                continue;
            }
            lastBatchIndexInBatch = i;
        }
        return lastBatchIndexInBatch;
    }
}
