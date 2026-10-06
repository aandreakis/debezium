/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;

public class JdbcCommitVisibilitySnapshotSourceTest {

    @Test
    void theSourceSwitchesItsConnectionToAutoCommitAndNeverCommits() throws Exception {
        final PostgresConnection connection = mock(PostgresConnection.class);
        final Connection jdbc = mock(Connection.class);
        when(connection.connection()).thenReturn(jdbc);
        when(jdbc.getAutoCommit()).thenReturn(false);
        when(connection.queryAndMap(anyString(), any())).thenReturn(Map.of(), Set.of());
        final JdbcCommitVisibilitySnapshotSource source = new JdbcCommitVisibilitySnapshotSource(connection);

        source.runningWriters();
        source.tablesWithDescendants();

        verify(jdbc, org.mockito.Mockito.atLeastOnce()).setAutoCommit(true);
        verify(jdbc, never()).commit();
        verify(jdbc, never()).rollback();
        verify(connection, never()).commit();
    }
}
