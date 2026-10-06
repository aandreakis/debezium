/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * A PostgreSQL specific read-only incremental snapshot change event source: it writes nothing to the database, so it
 * also runs on a hot standby.
 * <p>
 * <b>Prerequisites</b>
 * <ul>
 *      <li> PostgreSQL version >= 13 </li>
 * </ul>
 * </p>
 * <p>
 * <b>How a chunk is snapshotted</b>
 * <ul>
 *      <li> all snapshot work runs on the replication stream's thread; a chunk is read at a transaction commit, an
 *      idle poll, the connector start or a snapshot signal, and only when every transaction whose changes were already
 *      emitted is visible to new snapshots ({@link PostgresChunkReadGate}, {@link PostgresCommitVisibilityGuard});
 *      otherwise the read is retried later while streaming continues</li>
 *      <li> the chunk is read into the window buffer keyed by primary keys</li>
 *      <li> right after the chunk query, the window's release position is read: on a primary the WAL insert position,
 *      moved back over a page header that no record follows yet; on a standby the received position when it is at or
 *      after the replay position, otherwise the replay position, which the stream must then pass</li>
 * </ul>
 * </p>
 * <p>
 * <b>During the subsequent streaming</b>
 * <ul>
 *      <li> every event processed after the read removes the buffered row with the same key</li>
 *      <li> at the first commit whose commit record ends at or after the release position, or on an idle stream once
 *      the server has sent the stream up to it, the rest of the window's buffer is emitted and the next chunk is read.
 *      Every transaction the chunk query saw has been processed by then</li>
 * </ul>
 * </p>
 *
 * @author Mario Fiore Vitale
 */
public class PostgresReadOnlyIncrementalSnapshotChangeEventSource<P extends PostgresPartition>
        extends AbstractIncrementalSnapshotChangeEventSource<P, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);

    private static final String RELEASE_POSITION = "SELECT pg_is_in_recovery(),"
            + " CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_insert_lsn()::text END,"
            + " CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text END,"
            + " CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()::text END";
    private static final String WAL_LAYOUT = "SELECT current_setting('wal_block_size')::bigint,"
            + " (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'wal_segment_size')";
    /** {@code SizeOfXLogShortPHD} and {@code SizeOfXLogLongPHD} on 64-bit builds. */
    static final long SHORT_PAGE_HEADER = 24;
    static final long LONG_PAGE_HEADER = 40;

    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final PostgresChunkReadGate gate;
    private final Supplier<Lsn> serverPosition;
    private long walBlockSize;
    private long walSegmentSize;

    public PostgresReadOnlyIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                JdbcConnection jdbcConnection,
                                                                EventDispatcher<P, TableId> dispatcher,
                                                                DatabaseSchema<?> databaseSchema,
                                                                Clock clock,
                                                                SnapshotProgressListener<P> progressListener,
                                                                DataChangeEventListener<P> dataChangeEventListener,
                                                                NotificationService<P, ? extends OffsetContext> notificationService) {
        this(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService, () -> null);
    }

    /**
     * @param serverPosition how far the server has sent the replication stream, as far as the connector has read it
     *            (see {@link PostgresStreamingChangeEventSource#lastServerPosition()}); releases windows on an idle
     *            stream
     */
    public PostgresReadOnlyIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                JdbcConnection jdbcConnection,
                                                                EventDispatcher<P, TableId> dispatcher,
                                                                DatabaseSchema<?> databaseSchema,
                                                                Clock clock,
                                                                SnapshotProgressListener<P> progressListener,
                                                                DataChangeEventListener<P> dataChangeEventListener,
                                                                NotificationService<P, ? extends OffsetContext> notificationService,
                                                                Supplier<Lsn> serverPosition) {
        this(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService, serverPosition,
                PostgresChunkReadGate.create(config, (PostgresConnection) jdbcConnection));
    }

    PostgresReadOnlyIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                         JdbcConnection jdbcConnection,
                                                         EventDispatcher<P, TableId> dispatcher,
                                                         DatabaseSchema<?> databaseSchema,
                                                         Clock clock,
                                                         SnapshotProgressListener<P> progressListener,
                                                         DataChangeEventListener<P> dataChangeEventListener,
                                                         NotificationService<P, ? extends OffsetContext> notificationService,
                                                         Supplier<Lsn> serverPosition,
                                                         PostgresChunkReadGate gate) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.schema = (PostgresSchema) databaseSchema;
        this.serverPosition = serverPosition;
        this.gate = gate;
    }

    private PostgresReadOnlyIncrementalSnapshotContext<TableId> getContext() {
        return (PostgresReadOnlyIncrementalSnapshotContext<TableId>) context;
    }

    @Override
    public void init(P partition, OffsetContext offsetContext) {
        gate.start();
        super.init(partition, offsetContext);
    }

    @Override
    protected boolean isReadyForChunkRead(P partition, OffsetContext offsetContext) {
        return gate.readyForChunkRead();
    }

    @Override
    protected void readChunk(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.queue("chunk read", () -> readChunk(partition, offsetContext));
            return;
        }
        if (getContext() != null && getContext().isWindowOpen()) {
            LOGGER.debug("The previous chunk's window is open; the next chunk is read when it is released");
            return;
        }
        super.readChunk(partition, offsetContext);
    }

    @Override
    public void closeWindow(P partition, String id, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.queue("window close", () -> closeWindow(partition, id, offsetContext));
            return;
        }
        super.closeWindow(partition, id, offsetContext);
    }

    @Override
    public void pauseSnapshot(P partition, OffsetContext offsetContext) {
        if (!gate.onEventThread()) {
            gate.queue("pause", () -> pauseSnapshot(partition, offsetContext));
            return;
        }
        super.pauseSnapshot(partition, offsetContext);
    }

    @Override
    public void resumeSnapshot(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.queue("resume", () -> resumeSnapshot(partition, offsetContext));
            return;
        }
        super.resumeSnapshot(partition, offsetContext);
    }

    @Override
    public void addDataCollectionNamesToSnapshot(SignalPayload<P> signalPayload, SnapshotConfiguration snapshotConfiguration) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.queue("start", () -> addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration));
            return;
        }
        super.addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration);
    }

    @Override
    public void requestStopSnapshot(P partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionIds) {
        if (!gate.onEventThread()) {
            gate.queue("stop", () -> requestStopSnapshot(partition, offsetContext, additionalData, dataCollectionIds));
            return;
        }
        super.requestStopSnapshot(partition, offsetContext, additionalData, dataCollectionIds);
    }

    @Override
    protected void emitWindowOpen(P partition, OffsetContext offsetContext) {
        // The read decision is the window's start: every event processed from now on is compared with the buffer.
        LOGGER.trace("Reading chunk {}", getContext().currentChunkId());
    }

    @Override
    protected void emitWindowClose(P partition, OffsetContext offsetContext) {
        try {
            readReleasePosition();
        }
        catch (SQLException | RuntimeException e) {
            // Without a release position the buffered rows cannot be sent safely: read the chunk again later.
            LOGGER.warn("Reading the release position of incremental snapshot chunk {} failed; the chunk is read again later", getContext().currentChunkId(), e);
            window.clear();
            getContext().revertChunk();
            gate.retryLater();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(P partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException {
        context = (IncrementalSnapshotContext<TableId>) offsetContext.getIncrementalSnapshotContext();
        if (getContext() == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        gate.recordEmitted(offsetContext);
        if (!window.isEmpty()) {
            LOGGER.trace("Deduplicating");
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    public void processTransactionCommittedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
        gate.transactionCommitted();
        advance(partition, offsetContext, true);
    }

    @Override
    public void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {
        advance(partition, offsetContext, false);
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        gate.snapshotEnded();
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        schema.refreshFromIncrementalSnapshot(jdbcConnection, table.id());
        return schema.tableFor(table.id());
    }

    /**
     * At a commit or an idle poll on the stream's thread: run queued snapshot work; release the open window once the
     * stream has been read up to its release position and read the next chunk, or run a chunk read that was put off.
     */
    @SuppressWarnings("unchecked")
    private void advance(P partition, OffsetContext offsetContext, boolean atCommit) throws InterruptedException {
        if (!gate.onEventThread()) {
            return;
        }
        gate.runQueued();
        gate.maintain();
        if (offsetContext == null) {
            return;
        }
        context = (IncrementalSnapshotContext<TableId>) offsetContext.getIncrementalSnapshotContext();
        if (getContext() == null) {
            return;
        }
        if (getContext().isWindowOpen()) {
            final boolean due = atCommit
                    ? getContext().releaseDueAtCommit(((PostgresOffsetContext) offsetContext).lastCommitLsn())
                    : getContext().releaseDueWhenIdle(serverPosition.get());
            if (due) {
                LOGGER.trace("Stream reached the release position {}", getContext().getReleasePosition());
                getContext().releaseWindow();
                sendWindowEvents(partition, offsetContext);
                readChunk(partition, offsetContext);
            }
            return;
        }
        if (gate.retryDue()) {
            readChunk(partition, offsetContext);
        }
    }

    private void readReleasePosition() throws SQLException {
        final Object[] row = jdbcConnection.queryAndMap(RELEASE_POSITION, jdbcConnection.singleResultMapper(
                rs -> new Object[]{ rs.getBoolean(1), rs.getString(2), rs.getString(3), rs.getString(4) },
                "Unable to read the current WAL position"));
        // End the chunk query's transaction, so the connection does not stay idle in a transaction.
        jdbcConnection.commit();
        final boolean inRecovery = (Boolean) row[0];
        if (!inRecovery) {
            final Lsn insert = Lsn.valueOf((String) row[1]);
            ensureWalLayout();
            getContext().chunkRead(Lsn.valueOf(withoutTrailingPageHeader(insert.asLong(), walBlockSize, walSegmentSize)), false);
            return;
        }
        if (row[2] == null) {
            throw new SQLException("The standby has not replayed any WAL yet");
        }
        final Lsn replay = Lsn.valueOf((String) row[2]);
        final Lsn received = row[3] == null ? null : Lsn.valueOf((String) row[3]);
        if (received != null && received.compareTo(replay) >= 0) {
            // Every record that is or was in replay was received and flushed first, so it ends at or before this.
            getContext().chunkRead(received, false);
        }
        else {
            // A record in replay can be visible already and end after the replay position: the stream must pass it.
            getContext().chunkRead(replay, true);
        }
    }

    /**
     * The insert position points past a page header when the last record ends exactly at a page boundary, while the
     * stream's position stops at the record's end. Moves such a position back to the boundary.
     */
    static long withoutTrailingPageHeader(long insertPosition, long blockSize, long segmentSize) {
        if (insertPosition % segmentSize == LONG_PAGE_HEADER) {
            return insertPosition - LONG_PAGE_HEADER;
        }
        if (insertPosition % blockSize == SHORT_PAGE_HEADER) {
            return insertPosition - SHORT_PAGE_HEADER;
        }
        return insertPosition;
    }

    private void ensureWalLayout() throws SQLException {
        if (walBlockSize > 0) {
            return;
        }
        final long[] layout = jdbcConnection.queryAndMap(WAL_LAYOUT, jdbcConnection.singleResultMapper(
                rs -> new long[]{ rs.getLong(1), rs.getLong(2) }, "Unable to read the WAL layout"));
        jdbcConnection.commit();
        walBlockSize = layout[0];
        walSegmentSize = layout[1];
    }
}
