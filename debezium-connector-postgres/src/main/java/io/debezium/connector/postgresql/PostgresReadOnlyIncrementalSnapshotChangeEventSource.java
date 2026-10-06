/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotChangeEventSource;
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
 * A PostgreSQL specific read-only incremental snapshot change event source: it writes nothing to the database.
 * <p>
 * <b>Prerequisites</b>
 * <ul>
 *      <li> PostgreSQL version >= 13 </li>
 * </ul>
 * </p>
 * <p>
 * <b>How a chunk is snapshotted</b>
 * <ul>
 *      <li> a chunk is read on the replication stream's thread, at a transaction commit, an idle heartbeat, the
 *      connector start or a snapshot signal, and only when every transaction whose changes were already emitted is
 *      visible to new snapshots ({@link PostgresChunkReadGate}, {@link PostgresCommitVisibilityGuard}); otherwise
 *      the read is retried later while streaming continues</li>
 *      <li> the chunk is read into the window buffer keyed by primary keys</li>
 *      <li> right after the chunk query, the current WAL position is read: the insert position on a primary, the
 *      replay position on a standby; it is the window's release position</li>
 * </ul>
 * </p>
 * <p>
 * <b>During the subsequent streaming</b>
 * <ul>
 *      <li> every event processed after the read removes the buffered row with the same key</li>
 *      <li> at the first commit whose position is at or after the release position, or at an idle heartbeat once the
 *      server has sent the stream up to it, the rest of the window's buffer is emitted and the next chunk is read.
 *      Every transaction the chunk query saw committed before the release position, so it has been processed by
 *      then</li>
 * </ul>
 * </p>
 *
 * @author Mario Fiore Vitale
 */
public class PostgresReadOnlyIncrementalSnapshotChangeEventSource<P extends PostgresPartition>
        extends AbstractIncrementalSnapshotChangeEventSource<P, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);

    private static final String FORCE_NEW_TRANSACTION = "SELECT (CASE pg_is_in_recovery() WHEN 't' THEN NULL::xid8 ELSE pg_current_xact_id() END) AS txid;";
    private static final String CURRENT_WAL_POSITION = "SELECT (CASE pg_is_in_recovery() WHEN 't' THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_insert_lsn() END)::text";

    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final PostgresChunkReadGate gate;
    private final Supplier<Lsn> serverPosition;

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

        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.schema = (PostgresSchema) databaseSchema;
        this.serverPosition = serverPosition;
        this.gate = new PostgresChunkReadGate(new PostgresCommitVisibilityGuard(
                new JdbcCommitVisibilitySnapshotSource(this.jdbcConnection),
                PostgresChunkReadGate.inPlaceWait(config.getSignalProcessorSemaphoreWait())));
    }

    @Override
    protected void preIncrementalSnapshotStart() {
        super.preIncrementalSnapshotStart();

        forceNewTransactionId();
    }

    private PostgresReadOnlyIncrementalSnapshotContext<TableId> getContext() {
        return (PostgresReadOnlyIncrementalSnapshotContext<TableId>) context;
    }

    @Override
    public void init(P partition, OffsetContext offsetContext) {
        gate.bindEventThread();
        super.init(partition, offsetContext);
    }

    @Override
    protected boolean isReadyForChunkRead(P partition, OffsetContext offsetContext) {
        return gate.readyForChunkRead();
    }

    @Override
    protected void readChunk(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.deferReadToEventThread();
            return;
        }
        super.readChunk(partition, offsetContext);
    }

    @Override
    protected void emitWindowOpen(P partition, OffsetContext offsetContext) {
        // The read decision is the window's start: every event processed from now on is compared with the buffer.
        LOGGER.trace("Reading chunk {}", getContext().currentChunkId());
    }

    @Override
    protected void emitWindowClose(P partition, OffsetContext offsetContext) {
        getContext().chunkRead(currentWalPosition());
    }

    @Override
    public void processMessage(P partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) throws InterruptedException {

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
        if (offsetContext == null) {
            return;
        }
        advance(partition, offsetContext, ((PostgresOffsetContext) offsetContext).lastCommitLsn());
    }

    @Override
    public void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {
        advance(partition, offsetContext, serverPosition.get());
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
     * At a commit or an idle heartbeat on the stream's thread: release the open window once the stream has been read
     * up to its release position and read the next chunk, or run a chunk read that was put off.
     */
    private void advance(P partition, OffsetContext offsetContext, Lsn streamPosition) throws InterruptedException {
        if (offsetContext == null || getContext() == null || !gate.onEventThread()) {
            return;
        }
        gate.pruneIfOversized();
        if (getContext().isWindowOpen()) {
            if (getContext().releaseDue(streamPosition)) {
                LOGGER.trace("Stream at {} reached the release position {}", streamPosition, getContext().getReleasePosition());
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

    private Lsn currentWalPosition() {
        try {
            final Lsn position = jdbcConnection.queryAndMap(CURRENT_WAL_POSITION,
                    jdbcConnection.singleResultMapper(rs -> Lsn.valueOf(rs.getString(1)), "Unable to read the current WAL position"));
            LOGGER.trace("Release position {}", position);
            return position;
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    private void forceNewTransactionId() {
        try {
            jdbcConnection.query(FORCE_NEW_TRANSACTION, rs -> {
                if (rs.next()) {
                    String txId = rs.getString(1);
                    if (txId != null) {
                        LOGGER.trace("Created new transaction ID {}", txId);
                    }
                    else {
                        LOGGER.trace("Skipping transaction ID assignment on hot standby");
                    }
                }
            });
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

}
