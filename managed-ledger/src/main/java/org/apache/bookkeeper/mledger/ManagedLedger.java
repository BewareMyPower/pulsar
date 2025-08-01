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
package org.apache.bookkeeper.mledger;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

/**
 * A ManagedLedger it's a superset of a BookKeeper ledger concept.
 *
 * <p/>It mimics the concept of an appender log that:
 *
 * <ul>
 * <li>has a unique name (chosen by clients) by which it can be created/opened/deleted</li>
 * <li>is always writable: if a writer process crashes, a new writer can re-open the ManagedLedger and continue writing
 * into it</li>
 * <li>has multiple persisted consumers (see {@link ManagedCursor}), each of them with an associated position</li>
 * <li>when all the consumers have processed all the entries contained in a Bookkeeper ledger, the ledger is
 * deleted</li>
 * </ul>
 *
 * <p/>Caveats:
 * <ul>
 * <li>A single ManagedLedger can only be open once at any time. Implementation can protect double access from the same
 * VM, but accesses from different machines to the same ManagedLedger need to be avoided through an external source of
 * coordination.</li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedLedger {

    /**
     * @return the unique name of this ManagedLedger
     */
    String getName();

    /**
     * Append a new entry to the end of a managed ledger.
     *
     * @param data
     *            data entry to be persisted
     * @return the Position at which the entry has been inserted
     * @throws ManagedLedgerException
     */
    Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException;

    /**
     * Append a new entry to the end of a managed ledger.
     *
     * @param data
     *            data entry to be persisted
     * @param numberOfMessages
     *            numberOfMessages of entry
     * @return the Position at which the entry has been inserted
     * @throws ManagedLedgerException
     */
    Position addEntry(byte[] data, int numberOfMessages) throws InterruptedException, ManagedLedgerException;

    /**
     * Append a new entry asynchronously.
     *
     * @see #addEntry(byte[])
     * @param data
     *            data entry to be persisted
     *
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncAddEntry(byte[] data, AddEntryCallback callback, Object ctx);

    /**
     * Append a new entry to the end of a managed ledger.
     *
     * @param data
     *            data entry to be persisted
     * @param offset
     *            offset in the data array
     * @param length
     *            number of bytes
     * @return the Position at which the entry has been inserted
     * @throws ManagedLedgerException
     */
    Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException;

    /**
     * Append a new entry to the end of a managed ledger.
     *
     * @param data
     *            data entry to be persisted
     * @param numberOfMessages
     *            numberOfMessages of entry
     * @param offset
     *            offset in the data array
     * @param length
     *            number of bytes
     * @return the Position at which the entry has been inserted
     * @throws ManagedLedgerException
     */
    Position addEntry(byte[] data, int numberOfMessages, int offset, int length) throws InterruptedException,
            ManagedLedgerException;

    /**
     * Append a new entry asynchronously.
     *
     * @see #addEntry(byte[])
     * @param data
     *            data entry to be persisted
     * @param offset
     *            offset in the data array
     * @param length
     *            number of bytes
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncAddEntry(byte[] data, int offset, int length, AddEntryCallback callback, Object ctx);

    /**
     * Append a new entry asynchronously.
     *
     * @see #addEntry(byte[])
     * @param data
     *            data entry to be persisted
     * @param numberOfMessages
     *            numberOfMessages of entry
     * @param offset
     *            offset in the data array
     * @param length
     *            number of bytes
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length, AddEntryCallback callback,
                       Object ctx);


    /**
     * Append a new entry asynchronously.
     *
     * @see #addEntry(byte[])
     * @param buffer
     *            buffer with the data entry
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx);

    /**
     * Append a new entry asynchronously.
     *
     * @see #addEntry(byte[])
     * @param buffer
     *            buffer with the data entry
     * @param numberOfMessages
     *            numberOfMessages for data entry
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AddEntryCallback callback, Object ctx);

    /**
     * Open a ManagedCursor in this ManagedLedger.
     *
     * <p/>If the cursors doesn't exist, a new one will be created and its position will be at the end of the
     * ManagedLedger.
     *
     * @param name
     *            the name associated with the ManagedCursor
     * @return the ManagedCursor
     * @throws ManagedLedgerException
     */
    ManagedCursor openCursor(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Open a ManagedCursor in this ManagedLedger.
     * <p>
     * If the cursors doesn't exist, a new one will be created and its position will be at the end of the ManagedLedger.
     *
     * @param name
     *            the name associated with the ManagedCursor
     * @param initialPosition
     *            if null, the cursor will be set at latest position when first created
     * @return the ManagedCursor
     * @throws ManagedLedgerException
     */
    ManagedCursor openCursor(String name, InitialPosition initialPosition) throws InterruptedException,
            ManagedLedgerException;

    /**
     * Open a ManagedCursor in this ManagedLedger.
     * <p>
     * If the cursors doesn't exist, a new one will be created and its position will be at the end of the ManagedLedger.
     *
     * @param name
     *            the name associated with the ManagedCursor
     * @param initialPosition
     *            if null, the cursor will be set at latest position when first created
     * @param properties
     *             user defined properties that will be attached to the first position of the cursor, if the open
     *             operation will trigger the creation of the cursor.
     * @param cursorProperties
     *            the properties for the Cursor
     * @return the ManagedCursor
     * @throws ManagedLedgerException
     */
    ManagedCursor openCursor(String name, InitialPosition initialPosition, Map<String, Long> properties,
                             Map<String, String> cursorProperties)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Creates a new cursor whose metadata is not backed by durable storage. A caller can treat the non-durable cursor
     * exactly like a normal cursor, with the only difference in that after restart it will not remember which entries
     * were deleted. Also it does not prevent data from being deleted.
     *
     * <p/>The cursor is anonymous and can be positioned on an arbitrary position.
     *
     * <p/>This method is not-blocking.
     *
     * @param startCursorPosition
     *            the position where the cursor should be initialized, or null to start from the current latest entry.
     *            When starting on a particular cursor position, the first entry to be returned will be the entry next
     *            to the specified position
     * @return the new NonDurableCursor
     */
    ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException;
    ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName) throws ManagedLedgerException;
    ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName, InitialPosition initialPosition,
                                      boolean isReadCompacted) throws ManagedLedgerException;

    /**
     * Delete a ManagedCursor asynchronously.
     *
     * @see #deleteCursor(String)
     * @param name
     *            the name associated with the ManagedCursor
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncDeleteCursor(String name, DeleteCursorCallback callback, Object ctx);

    /**
     * Remove a ManagedCursor from this ManagedLedger.
     *
     * <p/>If the cursor doesn't exist, the operation will still succeed.
     *
     * @param name
     *            the name associated with the ManagedCursor
     *
     * @throws ManagedLedgerException
     * @throws InterruptedException
     */
    void deleteCursor(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Remove a ManagedCursor from this ManagedLedger's waitingCursors.
     *
     * @param cursor the ManagedCursor
     */
    void removeWaitingCursor(ManagedCursor cursor);

    /**
     * Open a ManagedCursor asynchronously.
     *
     * @see #openCursor(String)
     * @param name
     *            the name associated with the ManagedCursor
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncOpenCursor(String name, OpenCursorCallback callback, Object ctx);

    /**
     * Open a ManagedCursor asynchronously.
     *
     * @see #openCursor(String)
     * @param name
     *            the name associated with the ManagedCursor
     * @param initialPosition
     *            if null, the cursor will be set at latest position when first created
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncOpenCursor(String name, InitialPosition initialPosition, OpenCursorCallback callback, Object ctx);

    /**
     * Open a ManagedCursor asynchronously.
     *
     * @see #openCursor(String)
     * @param name
     *            the name associated with the ManagedCursor
     * @param initialPosition
     *            if null, the cursor will be set at latest position when first created
     * @param cursorProperties
     *            the properties for the Cursor
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncOpenCursor(String name, InitialPosition initialPosition, Map<String, Long> properties,
                         Map<String, String> cursorProperties, OpenCursorCallback callback, Object ctx);

    /**
     * Get a list of all the cursors reading from this ManagedLedger.
     *
     * @return a list of cursors
     */
    Iterable<ManagedCursor> getCursors();

    /**
     * Get a list of all the active cursors reading from this ManagedLedger.
     *
     * @return a list of cursors
     */
    Iterable<ManagedCursor> getActiveCursors();

    /**
     * Get the total number of entries for this managed ledger.
     *
     * <p/>This is defined by the number of entries in all the BookKeeper ledgers that are being maintained by this
     * ManagedLedger.
     *
     * <p/>This method is non-blocking.
     *
     * @return the number of entries
     */
    long getNumberOfEntries();

    long getNumberOfEntries(Range<Position> range);

    /**
     * Get the total number of active entries for this managed ledger.
     *
     * <p/>This is defined by the number of non consumed entries in all the BookKeeper ledgers that are being maintained
     * by this ManagedLedger.
     *
     * <p/>This method is non-blocking.
     *
     * @return the number of entries
     */
    long getNumberOfActiveEntries();

    /**
     * Get the total sizes in bytes of the managed ledger, without accounting for replicas.
     *
     * <p/>This is defined by the sizes of all the BookKeeper ledgers that are being maintained by this ManagedLedger.
     *
     * <p/>This method is non-blocking.
     *
     * @return total size in bytes
     */
    long getTotalSize();

    /**
     * Get estimated total unconsumed or backlog size in bytes for the managed ledger, without accounting for replicas.
     *
     * @return estimated total backlog size
     */
    long getEstimatedBacklogSize();

    /**
     * Get the publishing time of the oldest message in the backlog.
     *
     * @return the publishing time of the oldest message
     */
    CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog();

    /**
     * Return the size of all ledgers offloaded to 2nd tier storage.
     */
    long getOffloadedSize();

    /**
     * Resets the exception thrown by the PayloadProcessor during an add entry operation to null.
     * <p>
     * **Context:** When an add entry operation fails due to an interceptor, all subsequent incoming add entry
     * operations will also fail. This behavior ensures message ordering and consistency.
     * <p>
     * **Important:** This method MUST only be called after all pending add operations are fully completed
     * (e.g., after a Topic is unfenced). Calling it prematurely will prevent the Managed Ledger (ML)
     * from being able to write indefinitely.
     * <p>
     * **Implementation Note:** Downstream projects that support the ML PayloadProcessor should implement
     * this method. Otherwise, do not implement it.
     */
    default void unfenceForInterceptorException() {
        // Default implementation does nothing
    }

    /**
     * Get last offloaded ledgerId. If no offloaded yet, it returns 0.
     *
     * @return last offloaded ledgerId
     */
    long getLastOffloadedLedgerId();

    /**
    * Get last suceessful offloaded timestamp. If no successful offload, it returns 0.
     *
     * @return last successful offloaded timestamp
     */
    long getLastOffloadedSuccessTimestamp();

    /**
     * Get last failed offloaded timestamp. If no failed offload, it returns 0.
     *
     * @return last failed offloaded timestamp
     */
    long getLastOffloadedFailureTimestamp();

    void asyncTerminate(TerminateCallback callback, Object ctx);

    CompletableFuture<Position> asyncMigrate();


    /**
     * Add a property to the specified LedgerInfo.
     *
     * @param ledgerId the ledger id
     * @param key      the key of the property to add
     * @param value    the value of the property to add
     * @return
     * @throws ManagedLedgerException.ManagedLedgerFencedException if the ledger is fenced
     * @throws ManagedLedgerException if the ledger is not found or persistent failure
     */
    CompletableFuture<Void> asyncAddLedgerProperty(long ledgerId, String key, String value);

    /**
     * Remove a property from the specified LedgerInfo.
     *
     * @param ledgerId the ledger id
     * @param key      the key of the property to remove
     * @return
     * @throws ManagedLedgerException.ManagedLedgerFencedException if the ledger is fenced
     * @throws ManagedLedgerException if the ledger is not found or persistent failure
     */
    CompletableFuture<Void> asyncRemoveLedgerProperty(long ledgerId, String key);

    /**
     * Get the value of the specified property from the specified LedgerInfo.
     *
     * @param ledgerId the ledger id
     * @param key      the key of the property to get
     * @return the value of the property
     * @throws ManagedLedgerException.ManagedLedgerFencedException if the ledger is fenced
     * @throws ManagedLedgerException if the ledger is not found or persistent failure
     */
    CompletableFuture<String> asyncGetLedgerProperty(long ledgerId, String key);

    /**
     * Terminate the managed ledger and return the last committed entry.
     *
     * <p/>Once the managed ledger is terminated, it will not accept any more write
     *
     * @return
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    Position terminate() throws InterruptedException, ManagedLedgerException;

    /**
     * Close the ManagedLedger.
     *
     * <p/>This will close all the underlying BookKeeper ledgers. All the ManagedCursors associated will be invalidated.
     *
     * @throws ManagedLedgerException
     */
    void close() throws InterruptedException, ManagedLedgerException;

    /**
     * Close the ManagedLedger asynchronously.
     *
     * @see #close()
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    void asyncClose(CloseCallback callback, Object ctx);

    /**
     * @return the managed ledger stats MBean
     */
    ManagedLedgerMXBean getStats();

    /**
     * Delete the ManagedLedger.
     *
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void delete() throws InterruptedException, ManagedLedgerException;

    /**
     * Async delete a ledger.
     *
     * @param callback
     * @param ctx
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void asyncDelete(DeleteLedgerCallback callback, Object ctx);

    /**
     * Offload as many entries before position as possible to longterm storage.
     *
     * @param pos the position before which entries will be offloaded
     * @return the earliest position which was not offloaded
     *
     * @see #asyncOffloadPrefix(Position,OffloadCallback,Object)
     */
    Position offloadPrefix(Position pos) throws InterruptedException, ManagedLedgerException;

    /**
     * Offload as many entries before position as possible to longterm storage.
     *
     * As internally, entries is stored in ledgers, and ledgers can only be operated on as a whole,
     * it is likely not possible to offload every entry before the passed in position. Only complete
     * ledgers will be offloaded. On completion a position will be passed to the callback. This
     * position is the earliest entry which was not offloaded.
     *
     * @param pos the position before which entries will be offloaded
     * @param callback a callback which will be supplied with the earliest unoffloaded position on
     *                 completion
     * @param ctx a context object which will be passed to the callback on completion
     */
    void asyncOffloadPrefix(Position pos, OffloadCallback callback, Object ctx);

    /**
     * Get the slowest consumer.
     *
     * @return the slowest consumer
     */
    ManagedCursor getSlowestConsumer();

    /**
     * Returns whether the managed ledger was terminated.
     */
    boolean isTerminated();

    /**
     * Returns whether the managed ledger was migrated.
     */
    boolean isMigrated();

    /**
     * Returns managed-ledger config.
     */
    ManagedLedgerConfig getConfig();

    /**
     * Updates managed-ledger config.
     *
     * @param config
     */
    void setConfig(ManagedLedgerConfig config);

    /**
     * Gets last confirmed entry of the managed ledger.
     *
     * @return the last confirmed entry id
     */
    Position getLastConfirmedEntry();

    /**
     * Signaling managed ledger that we can resume after BK write failure.
     */
    void readyToCreateNewLedger();

    /**
     * Returns managed-ledger's properties.
     *
     * @return key-values of properties
     */
    Map<String, String> getProperties();

    /**
     * Add key-value to propertiesMap.
     *
     * @param key key of property to add
     * @param value value of property to add
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException;

    /**
     * Async add key-value to propertiesMap.
     *
     * @param key      key of property to add
     * @param value    value of property to add
     * @param callback a callback which will be supplied with the newest properties in managedLedger.
     * @param ctx      a context object which will be passed to the callback on completion.
     **/
    void asyncSetProperty(String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx);

    /**
     * Delete the property by key.
     *
     * @param key key of property to delete
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void deleteProperty(String key) throws InterruptedException, ManagedLedgerException;

    /**
     * Async delete the property by key.
     *
     * @param key      key of property to delete
     * @param callback a callback which will be supplied with the newest properties in managedLedger.
     * @param ctx      a context object which will be passed to the callback on completion.
     */
    void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx);

    /**
     * Update managed-ledger's properties.
     *
     * @param properties key-values of properties
     * @throws InterruptedException
     * @throws ManagedLedgerException
     */
    void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException;

    /**
     * Async update managed-ledger's properties.
     *
     * @param properties key-values of properties.
     * @param callback   a callback which will be supplied with the newest properties in managedLedger.
     * @param ctx        a context object which will be passed to the callback on completion.
     */
    void asyncSetProperties(Map<String, String> properties, AsyncCallbacks.UpdatePropertiesCallback callback,
        Object ctx);

    /**
     * Trim consumed ledgers in background.
     * @param promise
     */
    void trimConsumedLedgersInBackground(CompletableFuture<?> promise);

    /**
     * Rollover cursors in background if needed.
     */
    default void rolloverCursorsInBackground() {}

    /**
     * If a ledger is lost, this ledger will be skipped after enabled "autoSkipNonRecoverableData", and the method is
     * used to delete information about this ledger in the ManagedCursor.
     */
    default void skipNonRecoverableLedger(long ledgerId){}

    /**
     * Roll current ledger if it is full.
     */
    @Deprecated
    void rollCurrentLedgerIfFull();

    /**
     * Find position by sequenceId.
     * */
    CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate);

    /**
     * Get the ManagedLedgerInterceptor for ManagedLedger.
     * */
    ManagedLedgerInterceptor getManagedLedgerInterceptor();

    /**
     * Get basic ledger summary.
     * will got null if corresponding ledger not exists.
     */
    CompletableFuture<LedgerInfo> getLedgerInfo(long ledgerId);

    /**
     * Get basic ledger summary.
     * will get {@link Optional#empty()} if corresponding ledger not exists.
     */
    Optional<LedgerInfo> getOptionalLedgerInfo(long ledgerId);

    /**
     * Truncate ledgers
     * The truncate operation will move all cursors to the end of the topic and delete all inactive ledgers.
     */
    CompletableFuture<Void> asyncTruncate();

    /**
     * Get managed ledger internal stats.
     *
     * @param includeLedgerMetadata the flag to control managed ledger internal stats include ledger metadata
     * @return the future of managed ledger internal stats
     */
    CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata);

    /**
     * Check current inactive ledger (based on {@link ManagedLedgerConfig#getInactiveLedgerRollOverTimeMs()} and
     * roll over that ledger if inactive.
     *
     * @return true if ledger is considered for rolling over
     */
    boolean checkInactiveLedgerAndRollOver();

    /**
     * Check if managed ledger should cache backlog reads.
     */
    void checkCursorsToCacheEntries();

    /**
     * Get managed ledger attributes.
     */
    default ManagedLedgerAttributes getManagedLedgerAttributes() {
        return new ManagedLedgerAttributes(this);
    }

    void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx);

    /**
     * Get all the managed ledgers.
     */
    NavigableMap<Long, LedgerInfo> getLedgersInfo();

    Position getNextValidPosition(Position position);

    Position getPreviousPosition(Position position);

    long getEstimatedBacklogSize(Position position);

    Position getPositionAfterN(Position startPosition, long n, PositionBound startRange);

    int getPendingAddEntriesCount();

    long getCacheSize();

    default CompletableFuture<Position> getLastDispatchablePosition(final Predicate<Entry> predicate,
                                                                    final Position startPosition) {
        return CompletableFuture.completedFuture(PositionFactory.EARLIEST);
    }

    Position getFirstPosition();

    /**
     * Get the timestamp in milliseconds of the last successful add entry operation.
     *
     * @return the last add entry time in milliseconds
     */
    default long getLastAddEntryTime() {
        return 0;
    }

    /**
     * Get the creation timestamp of the managed ledger metadata, or 0 if not available.
     *
     * @return the creation timestamp in milliseconds, or 0 if not available
     */
    default long getMetadataCreationTimestamp() {
        return 0;
    }
}
