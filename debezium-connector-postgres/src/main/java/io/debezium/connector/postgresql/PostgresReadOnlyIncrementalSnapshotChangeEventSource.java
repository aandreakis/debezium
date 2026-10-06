/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
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
 *      moved back over a page header that no record follows yet; on a standby the replay position, which the stream
 *      must pass, because the commit record of a transaction the query saw can still be in replay and end after it</li>
 * </ul>
 * </p>
 * <p>
 * <b>During the subsequent streaming</b>
 * <ul>
 *      <li> every event processed after the read removes the buffered row with the same key</li>
 *      <li> at the first commit whose commit record ends at or after the release position, or on an idle stream once
 *      the server has sent the stream up to it, the rest of the window's buffer is emitted and the next chunk is read;
 *      on a standby the stream must be past the release position. Every transaction the chunk query saw has been
 *      processed by then</li>
 *      <li> on an idle standby the stream stops at the replay position. The window is then also released when the
 *      standby's startup process, which replays WAL, is seen waiting between two records and the replay position is
 *      still the release position: a record that was in replay when the release position was read has been replayed
 *      completely by then, which would have moved the replay position past it. This is checked at most once a
 *      second. Seeing the startup process's wait event needs the privileges of {@code pg_read_all_stats}; without
 *      them such a window waits for the next WAL record to be replayed. While replay is paused, the window waits until
 *      it resumes</li>
 * </ul>
 * </p>
 * <p>
 * The release position and the startup process are read on a connection of its own in autocommit mode, so each
 * reading of {@code pg_stat_activity} is new.
 * </p>
 *
 * @author Mario Fiore Vitale
 */
public class PostgresReadOnlyIncrementalSnapshotChangeEventSource<P extends PostgresPartition>
        extends AbstractIncrementalSnapshotChangeEventSource<P, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);

    private static final String RELEASE_POSITION = "SELECT pg_is_in_recovery(),"
            + " CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_insert_lsn()::text END,"
            + " CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()::text END";
    static final String STARTUP_WAIT_EVENT = "SELECT pg_catalog.pg_has_role('pg_read_all_stats', 'USAGE'),"
            + " (SELECT wait_event FROM pg_catalog.pg_stat_activity WHERE backend_type = 'startup')";
    static final String REPLAY_POSITION = "SELECT pg_last_wal_replay_lsn()::text";
    /**
     * Wait events the startup process shows only between two records, after the replay position has moved to the end
     * of the last replayed record: waiting for streamed WAL, waiting to retry the WAL sources, and the apply delay
     * before a commit record is replayed.
     */
    static final Set<String> BETWEEN_RECORDS_WAIT_EVENTS = Set.of("RecoveryWalStream", "RecoveryRetrieveRetryInterval", "RecoveryApplyDelay");
    /** The idle check runs at most once a second; its failures are logged at most every 30 s. */
    static final long IDLE_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long IDLE_CHECK_FAILURE_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final String WAL_LAYOUT = "SELECT current_setting('wal_block_size')::bigint,"
            + " (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'wal_segment_size')";
    /** {@code SizeOfXLogShortPHD} and {@code SizeOfXLogLongPHD} on 64-bit builds. */
    static final long SHORT_PAGE_HEADER = 24;
    static final long LONG_PAGE_HEADER = 40;

    private final PostgresConnection jdbcConnection;
    private final PostgresConnection visibilityConnection;
    private final PostgresSchema schema;
    private final PostgresChunkReadGate gate;
    private final Supplier<Lsn> serverPosition;
    private final LongSupplier nanoClock;
    private long walBlockSize;
    private long walSegmentSize;
    private boolean startupProcessHiddenLogged;
    private long lastIdleCheckNanos;
    private boolean idleCheckDone;
    private long idleCheckFailureLoggedNanos;
    private boolean idleCheckFailureLogged;

    /**
     * @param serverPosition how far the server has sent the replication stream, as far as the connector has read it
     *            (see {@link PostgresStreamingChangeEventSource#lastServerPosition()}); releases windows on an idle
     *            stream
     * @param visibilityConnection a connection of its own for the commit-visibility check and the release position,
     *            never the main connection; the caller closes it
     */
    public PostgresReadOnlyIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                JdbcConnection jdbcConnection,
                                                                EventDispatcher<P, TableId> dispatcher,
                                                                DatabaseSchema<?> databaseSchema,
                                                                Clock clock,
                                                                SnapshotProgressListener<P> progressListener,
                                                                DataChangeEventListener<P> dataChangeEventListener,
                                                                NotificationService<P, ? extends OffsetContext> notificationService,
                                                                Supplier<Lsn> serverPosition,
                                                                PostgresConnection visibilityConnection) {
        this(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService, serverPosition,
                visibilityConnection, PostgresChunkReadGate.create(config, visibilityConnection), System::nanoTime);
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
                                                         PostgresConnection visibilityConnection,
                                                         PostgresChunkReadGate gate,
                                                         LongSupplier nanoClock) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.visibilityConnection = visibilityConnection;
        this.schema = (PostgresSchema) databaseSchema;
        this.serverPosition = serverPosition;
        this.gate = gate;
        this.nanoClock = nanoClock;
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
        // A read can move on to the next tables of the snapshot within the same window, so all of them count.
        return gate.readyForChunkRead(PostgresChunkReadGate.tablesLeft(context));
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
        gate.afterChunkRead(getContext() != null && getContext().snapshotRunning() && !getContext().isSnapshotPaused() && !getContext().isWindowOpen());
    }

    @Override
    public void closeWindow(P partition, String id, OffsetContext offsetContext) throws InterruptedException {
        gate.control("window close", () -> super.closeWindow(partition, id, offsetContext));
    }

    @Override
    public void pauseSnapshot(P partition, OffsetContext offsetContext) {
        controlWithoutInterrupt("pause", () -> super.pauseSnapshot(partition, offsetContext));
    }

    @Override
    public void resumeSnapshot(P partition, OffsetContext offsetContext) throws InterruptedException {
        gate.control("resume", () -> super.resumeSnapshot(partition, offsetContext));
    }

    @Override
    public void addDataCollectionNamesToSnapshot(SignalPayload<P> signalPayload, SnapshotConfiguration snapshotConfiguration) throws InterruptedException {
        gate.control("start", () -> super.addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration));
    }

    @Override
    public void requestStopSnapshot(P partition, OffsetContext offsetContext, Map<String, Object> additionalData, List<String> dataCollectionIds) {
        controlWithoutInterrupt("stop", () -> super.requestStopSnapshot(partition, offsetContext, additionalData, dataCollectionIds));
    }

    /** A control whose caller cannot be interrupted: an interrupt while the queue runs is kept for the thread. */
    private void controlWithoutInterrupt(String what, PostgresChunkReadGate.Work work) {
        try {
            gate.control(what, work);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Interrupted while running incremental snapshot {}", what);
        }
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
        gate.recordEmitted(offsetContext, dataCollectionId);
        if (gate.hasQueued()) {
            // Requests from other threads run before this event is compared, so they wait for no commit.
            gate.runQueued();
        }
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
                    : releaseDueWhenIdle(serverPosition.get());
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
        // End the chunk query's transaction, so the main connection does not stay idle in a transaction.
        jdbcConnection.commit();
        final Object[] row = autoCommit().queryAndMap(RELEASE_POSITION, visibilityConnection.singleResultMapper(
                rs -> new Object[]{ rs.getBoolean(1), rs.getString(2), rs.getString(3) },
                "Unable to read the current WAL position"));
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
        // A record in replay can be visible already and end after the replay position: the stream must pass it.
        getContext().chunkRead(Lsn.valueOf((String) row[2]), true);
    }

    /**
     * On an idle poll: whether the stream has reached the release position, or, on a standby, passed it, or sits at it
     * while the standby's startup process waits between two records with the replay position still there.
     */
    boolean releaseDueWhenIdle(Lsn sent) {
        if (getContext().releaseDueWhenIdle(sent)) {
            return true;
        }
        if (!getContext().waitsAtReleasePosition(sent)) {
            return false;
        }
        final long now = nanoClock.getAsLong();
        if (idleCheckDone && now - lastIdleCheckNanos < IDLE_CHECK_INTERVAL_NANOS) {
            return false;
        }
        idleCheckDone = true;
        lastIdleCheckNanos = now;
        try {
            return replayIdleAt(getContext().getReleasePosition());
        }
        catch (SQLException | RuntimeException e) {
            if (!idleCheckFailureLogged || now - idleCheckFailureLoggedNanos >= IDLE_CHECK_FAILURE_LOG_INTERVAL_NANOS) {
                idleCheckFailureLogged = true;
                idleCheckFailureLoggedNanos = now;
                LOGGER.warn("Reading the standby's replay state failed; the window is released once the stream passes {}", getContext().getReleasePosition(), e);
            }
            return false;
        }
    }

    /**
     * Whether the startup process is seen waiting between two records and, read after that, the replay position is
     * {@code releasePosition}. A record that was in replay when the release position was read has then been replayed
     * completely, and its end would be after {@code releasePosition}.
     */
    private boolean replayIdleAt(Lsn releasePosition) throws SQLException {
        final Object[] startup = autoCommit().queryAndMap(STARTUP_WAIT_EVENT, visibilityConnection.singleResultMapper(
                rs -> new Object[]{ rs.getBoolean(1), rs.getString(2) }, "Unable to read the startup process's wait event"));
        if (!(Boolean) startup[0]) {
            if (!startupProcessHiddenLogged) {
                startupProcessHiddenLogged = true;
                LOGGER.warn("The connector's user lacks the privileges of pg_read_all_stats, so it cannot see whether the standby's startup process is idle; "
                        + "on an idle standby an incremental snapshot chunk then waits until the next WAL record is replayed");
            }
            return false;
        }
        if (startup[1] == null || !BETWEEN_RECORDS_WAIT_EVENTS.contains((String) startup[1])) {
            return false;
        }
        final String replay = visibilityConnection.queryAndMap(REPLAY_POSITION, visibilityConnection.singleResultMapper(
                rs -> rs.getString(1), "Unable to read the replay position"));
        return replay != null && Lsn.valueOf(replay).equals(releasePosition);
    }

    /** The release connection in autocommit mode, so each statement sees new server state. */
    private PostgresConnection autoCommit() throws SQLException {
        if (!visibilityConnection.connection().getAutoCommit()) {
            visibilityConnection.connection().setAutoCommit(true);
        }
        return visibilityConnection;
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
        final long[] layout = autoCommit().queryAndMap(WAL_LAYOUT, visibilityConnection.singleResultMapper(
                rs -> new long[]{ rs.getLong(1), rs.getLong(2) }, "Unable to read the WAL layout"));
        walBlockSize = layout[0];
        walSegmentSize = layout[1];
    }
}
