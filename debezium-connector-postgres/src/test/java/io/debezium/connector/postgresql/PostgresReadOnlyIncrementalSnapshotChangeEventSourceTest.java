/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link PostgresReadOnlyIncrementalSnapshotChangeEventSource}.
 */
public class PostgresReadOnlyIncrementalSnapshotChangeEventSourceTest {

    private static final long BLOCK = 8192;
    private static final long SEGMENT = 16L * 1024 * 1024;

    @Test
    @FixFor("debezium/dbz#2431")
    void theSourceAssignsNoTransactionIdSoItRunsOnAHotStandby() throws Exception {
        for (Field field : PostgresReadOnlyIncrementalSnapshotChangeEventSource.class.getDeclaredFields()) {
            if (field.getType() == String.class && Modifier.isStatic(field.getModifiers())) {
                field.setAccessible(true);
                final String sql = (String) field.get(null);
                assertThat(sql).as(field.getName()).doesNotContain("pg_current_xact_id").doesNotContain("txid_current()");
            }
        }
        final Field releasePosition = PostgresReadOnlyIncrementalSnapshotChangeEventSource.class.getDeclaredField("RELEASE_POSITION");
        releasePosition.setAccessible(true);
        assertThat((String) releasePosition.get(null)).contains("pg_is_in_recovery()", "pg_last_wal_replay_lsn()", "pg_last_wal_receive_lsn()");
    }

    @Test
    void anInsertPositionRightAfterAPageHeaderMovesBackToThePageBoundary() {
        assertThat(PostgresReadOnlyIncrementalSnapshotChangeEventSource.withoutTrailingPageHeader(3 * BLOCK + 24, BLOCK, SEGMENT))
                .as("short header").isEqualTo(3 * BLOCK);
        assertThat(PostgresReadOnlyIncrementalSnapshotChangeEventSource.withoutTrailingPageHeader(5 * SEGMENT + 40, BLOCK, SEGMENT))
                .as("long header on the first page of a segment").isEqualTo(5 * SEGMENT);
        assertThat(PostgresReadOnlyIncrementalSnapshotChangeEventSource.withoutTrailingPageHeader(3 * BLOCK + 200, BLOCK, SEGMENT)).isEqualTo(3 * BLOCK + 200);
        assertThat(PostgresReadOnlyIncrementalSnapshotChangeEventSource.withoutTrailingPageHeader(3 * BLOCK, BLOCK, SEGMENT)).isEqualTo(3 * BLOCK);
    }

    @Test
    @SuppressWarnings("unchecked")
    void whenTheReleasePositionCannotBeReadTheBufferedChunkIsDroppedAndReadAgainLater() throws Exception {
        final AtomicLong clock = new AtomicLong();
        final PostgresChunkReadGate gate = new PostgresChunkReadGate(new PostgresChunkReadGateTest.ScriptedGuardBase(), clock::get);
        final PostgresConnection connection = mock(PostgresConnection.class);
        when(connection.queryAndMap(anyString(), any())).thenThrow(new SQLException("connection lost"));
        final TestSource source = new TestSource(connection, gate);
        final PostgresReadOnlyIncrementalSnapshotContext<TableId> context = new PostgresReadOnlyIncrementalSnapshotContext<>();
        context.addDataCollectionNamesToSnapshot("signal-1", List.of("public.a"), List.of(), "");
        context.sendEvent(new Object[]{ 0 });
        context.nextChunkPosition(new Object[]{ 10 });
        source.useContext(context);
        source.bufferRow();

        source.emitWindowClose(null, null);

        verify(connection).queryAndMap(anyString(), any());
        assertThat(source.bufferedRows()).isZero();
        assertThat(context.isWindowOpen()).isFalse();
        assertThat(context.chunkEndPosititon()).as("the chunk is read again from the last row sent").containsExactly(0);
        assertThat(gate.isRetryPending()).isTrue();
        assertThat(gate.retryDue()).isFalse();
        clock.addAndGet(1_000_000_000L);
        assertThat(gate.retryDue()).isTrue();
    }

    static final class TestSource extends PostgresReadOnlyIncrementalSnapshotChangeEventSource<PostgresPartition> {
        private static final Schema KEY_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

        @SuppressWarnings("unchecked")
        TestSource(PostgresConnection connection, PostgresChunkReadGate gate) {
            super(mock(RelationalDatabaseConnectorConfig.class), connection, null, mock(PostgresSchema.class), null, mock(SnapshotProgressListener.class), null,
                    mock(NotificationService.class, RETURNS_DEEP_STUBS), () -> null, gate);
        }

        void useContext(PostgresReadOnlyIncrementalSnapshotContext<TableId> context) {
            this.context = context;
        }

        void bufferRow() {
            window.put(new Struct(KEY_SCHEMA).put("id", 5), new Object[]{ 5 });
        }

        int bufferedRows() {
            return window.size();
        }
    }
}
