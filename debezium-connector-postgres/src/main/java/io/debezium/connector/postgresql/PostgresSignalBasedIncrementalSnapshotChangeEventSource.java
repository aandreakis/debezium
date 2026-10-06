/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
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
 * Custom PostgreSQL implementation of the {@link SignalBasedIncrementalSnapshotChangeEventSource} implementation
 * which performs an explicit schema refresh of a table prior to the incremental snapshot starting.
 * <p>
 * On a PostgreSQL primary a transaction reaches the replication stream before new snapshots see it, so a chunk query
 * can miss a change whose event was already emitted. This source therefore
 * <ul>
 * <li>reads a chunk only on the stream's thread and only when every transaction whose changes were emitted is visible
 * ({@link PostgresChunkReadGate}, {@link PostgresCommitVisibilityGuard}), and</li>
 * <li>lets every event processed after a chunk was read remove the buffered row with the same key, not only events
 * that follow the open marker: an event processed between the read and the open marker can belong to a transaction
 * the chunk query did not see.</li>
 * </ul>
 *
 * @author Chris Cranford
 */
public class PostgresSignalBasedIncrementalSnapshotChangeEventSource
        extends SignalBasedIncrementalSnapshotChangeEventSource<PostgresPartition, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSignalBasedIncrementalSnapshotChangeEventSource.class);

    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final PostgresChunkReadGate gate;

    public PostgresSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                   JdbcConnection jdbcConnection,
                                                                   EventDispatcher<PostgresPartition, TableId> dispatcher,
                                                                   DatabaseSchema<?> databaseSchema,
                                                                   Clock clock,
                                                                   SnapshotProgressListener<PostgresPartition> progressListener,
                                                                   DataChangeEventListener<PostgresPartition> dataChangeEventListener,
                                                                   NotificationService<PostgresPartition, ? extends OffsetContext> notificationService) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.schema = (PostgresSchema) databaseSchema;
        this.gate = new PostgresChunkReadGate(new PostgresCommitVisibilityGuard(
                new JdbcCommitVisibilitySnapshotSource(this.jdbcConnection),
                PostgresChunkReadGate.inPlaceWait(config.getSignalProcessorSemaphoreWait())));
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        schema.refreshFromIncrementalSnapshot(jdbcConnection, table.id());
        return schema.tableFor(table.id());
    }

    @Override
    public void init(PostgresPartition partition, OffsetContext offsetContext) {
        gate.bindEventThread();
        super.init(partition, offsetContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(PostgresPartition partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<TableId>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        gate.recordEmitted(offsetContext);
        // A non-empty window means a chunk was read and not yet sent. Every event processed after the read removes the
        // row with the same key, also before the open marker comes back through the stream.
        if (!window.isEmpty()) {
            deduplicateWindow(dataCollectionId, key);
        }
    }

    @Override
    protected boolean isReadyForChunkRead(PostgresPartition partition, OffsetContext offsetContext) {
        return gate.readyForChunkRead();
    }

    @Override
    protected void readChunk(PostgresPartition partition, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.deferReadToEventThread();
            return;
        }
        super.readChunk(partition, offsetContext);
    }

    @Override
    public void closeWindow(PostgresPartition partition, String id, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            gate.deferCloseToEventThread(id);
            return;
        }
        super.closeWindow(partition, id, offsetContext);
    }

    @Override
    public void processTransactionCommittedEvent(PostgresPartition partition, OffsetContext offsetContext) throws InterruptedException {
        runDeferredWork(partition, offsetContext);
    }

    @Override
    public void processHeartbeat(PostgresPartition partition, OffsetContext offsetContext) throws InterruptedException {
        runDeferredWork(partition, offsetContext);
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        gate.snapshotEnded();
    }

    /**
     * At a commit or an idle heartbeat on the stream's thread: run a window close or a chunk read that was put off.
     */
    @SuppressWarnings("unchecked")
    private void runDeferredWork(PostgresPartition partition, OffsetContext offsetContext) throws InterruptedException {
        if (offsetContext == null || !gate.onEventThread()) {
            return;
        }
        context = (IncrementalSnapshotContext<TableId>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            return;
        }
        final String closeId = gate.takePendingClose();
        if (closeId != null) {
            super.closeWindow(partition, closeId, offsetContext);
        }
        gate.pruneIfOversized();
        if (gate.retryDue()) {
            readChunk(partition, offsetContext);
        }
    }
}
