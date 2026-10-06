/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;

/**
 * {@link PostgresCommitVisibilityGuard.SnapshotSource} on a JDBC connection of its own, never the connector's main
 * connection: the main connection can hold a transaction that must stay open, such as the snapshot transaction of
 * catch-up streaming before a snapshot. The connection runs with autocommit, so each query sees a new snapshot and the
 * connection is never left idle in a transaction.
 */
public class JdbcCommitVisibilitySnapshotSource implements PostgresCommitVisibilityGuard.SnapshotSource {

    private static final String STATE = "SELECT pg_is_in_recovery(), pg_current_snapshot()::text";
    private static final String STATE_BEFORE_13 = "SELECT pg_is_in_recovery(), txid_current_snapshot()::text";
    private static final String RUNNING_WRITERS = "SELECT DISTINCT x.transactionid::text::bigint, n.nspname, c.relname"
            + " FROM pg_catalog.pg_locks x"
            + " JOIN pg_catalog.pg_locks r ON r.virtualtransaction = x.virtualtransaction AND r.locktype = 'relation' AND r.granted"
            + " AND r.mode NOT IN ('AccessShareLock', 'RowShareLock')"
            + " AND r.database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())"
            + " JOIN pg_catalog.pg_class c ON c.oid = r.relation AND c.relkind IN ('r', 'p') AND c.relpersistence <> 't'"
            + " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
            + " WHERE x.locktype = 'transactionid' AND x.mode = 'ExclusiveLock' AND x.granted";

    private static final String TABLES_WITH_DESCENDANTS = "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c"
            + " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
            + " WHERE c.relkind = 'p' OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent = c.oid)";

    private final PostgresConnection connection;
    private Boolean modern;

    public JdbcCommitVisibilitySnapshotSource(PostgresConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    @Override
    public PostgresCommitVisibilityGuard.ServerState currentState() throws SQLException {
        return withAutoCommit(() -> connection.queryAndMap(modern() ? STATE : STATE_BEFORE_13,
                connection.singleResultMapper(rs -> new PostgresCommitVisibilityGuard.ServerState(rs.getBoolean(1), PgSnapshot.valueOf(rs.getString(2))),
                        "Unable to read the current snapshot")));
    }

    @Override
    public Map<Long, Set<TableId>> runningWriters() throws SQLException {
        return withAutoCommit(() -> connection.queryAndMap(RUNNING_WRITERS, rs -> {
            final Map<Long, Set<TableId>> writers = new HashMap<>();
            while (rs.next()) {
                writers.computeIfAbsent(rs.getLong(1), id -> new HashSet<>()).add(new TableId(null, rs.getString(2), rs.getString(3)));
            }
            return writers;
        }));
    }

    @Override
    public Set<TableId> tablesWithDescendants() throws SQLException {
        return withAutoCommit(() -> connection.queryAndMap(TABLES_WITH_DESCENDANTS, rs -> {
            final Set<TableId> tables = new HashSet<>();
            while (rs.next()) {
                tables.add(new TableId(null, rs.getString(1), rs.getString(2)));
            }
            return tables;
        }));
    }

    @FunctionalInterface
    private interface Query<T> {
        T run() throws SQLException;
    }

    private <T> T withAutoCommit(Query<T> query) throws SQLException {
        final Connection jdbc = connection.connection();
        if (!jdbc.getAutoCommit()) {
            jdbc.setAutoCommit(true);
        }
        return query.run();
    }

    private boolean modern() throws SQLException {
        if (modern == null) {
            modern = connection.connection().getMetaData().getDatabaseMajorVersion() >= 13;
        }
        return modern;
    }
}
