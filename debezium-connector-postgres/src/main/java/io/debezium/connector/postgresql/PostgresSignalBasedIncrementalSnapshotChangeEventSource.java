/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
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
 * <li>runs all snapshot work on the stream's thread and reads a chunk only when every transaction whose changes were
 * emitted is visible ({@link PostgresChunkReadGate}, {@link PostgresCommitVisibilityGuard}),</li>
 * <li>lets every event processed after a chunk was read remove the buffered row with the same key, not only events
 * that follow the open marker: an event processed between the read and the open marker can belong to a transaction
 * the chunk query did not see, and</li>
 * <li>reads no new chunk while the rows of the previous one are buffered and not yet sent.</li>
 * </ul>
 * The class is generic in the partition type so that it can override {@code processMessage(Partition, ...)} as
 * declared by its superclass.
 *
 * @author Chris Cranford
 */
public class PostgresSignalBasedIncrementalSnapshotChangeEventSource<P extends Partition>
        extends SignalBasedIncrementalSnapshotChangeEventSource<P, TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSignalBasedIncrementalSnapshotChangeEventSource.class);

    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;
    private final PostgresChunkReadGate gate;

    public PostgresSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                                   JdbcConnection jdbcConnection,
                                                                   EventDispatcher<P, TableId> dispatcher,
                                                                   DatabaseSchema<?> databaseSchema,
                                                                   Clock clock,
                                                                   SnapshotProgressListener<P> progressListener,
                                                                   DataChangeEventListener<P> dataChangeEventListener,
                                                                   NotificationService<P, ? extends OffsetContext> notificationService) {
        this(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService,
                PostgresChunkReadGate.create(config, (PostgresConnection) jdbcConnection));
    }

    PostgresSignalBasedIncrementalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig config,
                                                            JdbcConnection jdbcConnection,
                                                            EventDispatcher<P, TableId> dispatcher,
                                                            DatabaseSchema<?> databaseSchema,
                                                            Clock clock,
                                                            SnapshotProgressListener<P> progressListener,
                                                            DataChangeEventListener<P> dataChangeEventListener,
                                                            NotificationService<P, ? extends OffsetContext> notificationService,
                                                            PostgresChunkReadGate gate) {
        super(config, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
        this.jdbcConnection = (PostgresConnection) jdbcConnection;
        this.schema = (PostgresSchema) databaseSchema;
        this.gate = gate;
    }

    @Override
    protected Table refreshTableSchema(Table table) throws SQLException {
        LOGGER.debug("Refreshing table '{}' schema for incremental snapshot.", table.id());
        schema.refreshFromIncrementalSnapshot(jdbcConnection, table.id());
        return schema.tableFor(table.id());
    }

    @Override
    public void init(P partition, OffsetContext offsetContext) {
        gate.start();
        super.init(partition, offsetContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processMessage(Partition partition, DataCollectionId dataCollectionId, Object key, OffsetContext offsetContext) {
        context = (IncrementalSnapshotContext<TableId>) offsetContext.getIncrementalSnapshotContext();
        if (context == null) {
            LOGGER.warn("Context is null, skipping message processing");
            return;
        }
        gate.recordEmitted(offsetContext, dataCollectionId);
        // A non-empty window means a chunk was read and not yet sent. Every event processed after the read removes the
        // row with the same key, also before the open marker comes back through the stream.
        if (!window.isEmpty()) {
            deduplicateWindow(dataCollectionId, key);
        }
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
        if (!window.isEmpty()) {
            LOGGER.debug("The previous chunk is buffered and not yet sent; the next chunk is read when its window closes");
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
    public void processTransactionCommittedEvent(P partition, OffsetContext offsetContext) throws InterruptedException {
        gate.transactionCommitted();
        runQueuedWork(partition, offsetContext);
    }

    @Override
    public void processHeartbeat(P partition, OffsetContext offsetContext) throws InterruptedException {
        runQueuedWork(partition, offsetContext);
    }

    @Override
    protected void postIncrementalSnapshotCompleted() {
        gate.snapshotEnded();
    }

    /**
     * At a commit or an idle poll on the stream's thread: run queued snapshot work, keep the tracked set small, and
     * retry a chunk read that was put off.
     */
    @SuppressWarnings("unchecked")
    private void runQueuedWork(P partition, OffsetContext offsetContext) throws InterruptedException {
        if (!gate.onEventThread()) {
            return;
        }
        gate.runQueued();
        gate.maintain();
        if (offsetContext == null) {
            return;
        }
        context = (IncrementalSnapshotContext<TableId>) offsetContext.getIncrementalSnapshotContext();
        if (context != null && gate.retryDue()) {
            readChunk(partition, offsetContext);
        }
    }
}
