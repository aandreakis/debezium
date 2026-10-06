/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

public class PostgresSignalBasedIncrementalSnapshotChangeEventSourceTest {

    private final AtomicLong clock = new AtomicLong();
    private final CountingGuard guard = new CountingGuard();
    private final PostgresChunkReadGate gate = new PostgresChunkReadGate(guard, clock::get);
    private PostgresConnection jdbcConnection;
    private OffsetContext offsetContext;
    private SignalBasedIncrementalSnapshotContext<TableId> context;
    private TestSource source;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        jdbcConnection = mock(PostgresConnection.class);
        source = new TestSource(mock(RelationalDatabaseConnectorConfig.class), jdbcConnection, mock(SnapshotProgressListener.class),
                mock(NotificationService.class, RETURNS_DEEP_STUBS), gate);
        context = new SignalBasedIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a", "public.b"), List.of(), "");
        offsetContext = mock(OffsetContext.class);
        doReturn(context).when(offsetContext).getIncrementalSnapshotContext();
        // Bind the source to this thread, as the streaming source does when it starts.
        source.init(null, null);
    }

    @Test
    void aReadRequestedOnAnotherThreadRunsOnTheStreamsThreadAtTheNextIdlePoll() throws Exception {
        guard.visible = false;
        runOnAnotherThread(() -> source.readChunk(null, offsetContext));
        assertThat(guard.checks).as("nothing ran on the other thread").isZero();

        source.processHeartbeat(null, offsetContext);

        assertThat(guard.checks).isEqualTo(1);
        assertThat(guard.checkThread).isSameAs(Thread.currentThread());
        assertThat(guard.readTables).as("the current table and those that follow").containsExactlyInAnyOrder(TableId.parse("public.a"), TableId.parse("public.b"));
    }

    @Test
    void noNewChunkIsReadWhileThePreviousChunkIsBuffered() throws Exception {
        source.bufferRow();

        source.readChunk(null, offsetContext);

        assertThat(guard.checks).isZero();
        verify(jdbcConnection, never()).commit();
    }

    @Test
    void aFailedReadIsRetriedSoTheRestOfTheSnapshotMovesOn() throws Exception {
        guard.visible = false;
        source.readChunk(null, offsetContext);
        assertThat(guard.checks).isEqualTo(1);

        // The snapshot is paused and resumed before the retry is due; the resume reads and its check passes. The read
        // then fails on the connection, and the shared code skips the first table without opening a window.
        when(jdbcConnection.commit()).thenThrow(new SQLException("read failed"));
        source.pauseSnapshot(null, offsetContext);
        guard.visible = true;
        source.resumeSnapshot(null, offsetContext);
        assertThat(guard.checks).isEqualTo(2);
        assertThat(context.snapshotRunning()).as("the second table is left").isTrue();

        clock.addAndGet(1_000_000_000L);
        source.processTransactionCommittedEvent(null, offsetContext);
        assertThat(guard.checks).as("the second table is read after the retry delay").isEqualTo(3);
    }

    @Test
    void aRequestQueuedOnAnotherThreadRunsBeforeTheStreamsNextEvent() throws Exception {
        runOnAnotherThread(() -> source.pauseSnapshot(null, offsetContext));
        assertThat(context.isSnapshotPaused()).isFalse();

        source.processMessage(null, context.currentDataCollectionId().getId(), TestSource.key(7), offsetContext);

        assertThat(context.isSnapshotPaused()).as("no commit or idle poll needed").isTrue();
    }

    @Test
    void aPauseRequestedOnAnotherThreadTakesEffectOnTheStreamsThread() throws Exception {
        runOnAnotherThread(() -> source.pauseSnapshot(null, offsetContext));
        assertThat(context.isSnapshotPaused()).isFalse();

        source.processTransactionCommittedEvent(null, offsetContext);

        assertThat(context.isSnapshotPaused()).isTrue();
    }

    @Test
    void aResumeOnTheStreamsThreadTakesEffectAfterAPauseRequestedEarlierOnAnotherThread() throws Exception {
        // The resume reads a chunk; the read fails on the connection, as in the test above, which keeps the test short.
        when(jdbcConnection.commit()).thenThrow(new SQLException("read failed"));
        runOnAnotherThread(() -> source.pauseSnapshot(null, offsetContext));
        assertThat(context.isSnapshotPaused()).isFalse();

        source.resumeSnapshot(null, offsetContext);

        assertThat(context.isSnapshotPaused()).as("the earlier pause ran first, then the resume").isFalse();
        assertThat(guard.checks).as("the resume read a chunk").isEqualTo(1);
        source.processTransactionCommittedEvent(null, offsetContext);
        assertThat(context.isSnapshotPaused()).as("no pause left in the queue").isFalse();
    }

    @Test
    void aRowChangedAfterTheReadIsDroppedBeforeTheOpenMarkerComesBack() {
        source.bufferRow();
        assertThat(source.bufferedRows()).isEqualTo(1);

        source.processMessage(null, context.currentDataCollectionId().getId(), TestSource.key(1), offsetContext);

        assertThat(source.bufferedRows()).as("removed before the open marker came back").isZero();
    }

    private static void runOnAnotherThread(ThrowingRunnable work) throws InterruptedException {
        final Thread other = new Thread(() -> {
            try {
                work.run();
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        other.start();
        other.join();
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Counts checks; passes them unless told otherwise. */
    static final class CountingGuard extends PostgresChunkReadGateTest.ScriptedGuardBase {
        int checks;
        boolean visible = true;
        Thread checkThread;
        Collection<TableId> readTables;

        @Override
        public boolean awaitVisible(Collection<TableId> readTables) {
            checks++;
            checkThread = Thread.currentThread();
            this.readTables = readTables;
            return visible;
        }
    }

    static final class TestSource extends PostgresSignalBasedIncrementalSnapshotChangeEventSource<PostgresPartition> {
        private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

        @SuppressWarnings("unchecked")
        TestSource(RelationalDatabaseConnectorConfig config, PostgresConnection connection, SnapshotProgressListener<PostgresPartition> progressListener,
                   NotificationService<PostgresPartition, ? extends OffsetContext> notificationService, PostgresChunkReadGate gate) {
            super(config, connection, null, mock(PostgresSchema.class), null, progressListener, null, notificationService, gate);
        }

        static Struct key(int id) {
            return new Struct(KEY_SCHEMA).put("id", id);
        }

        void bufferRow() {
            window.put(key(1), new Object[]{ 1 });
        }

        int bufferedRows() {
            return window.size();
        }
    }
}
