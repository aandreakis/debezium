/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;

/**
 * {@link PostgresCommitVisibilityGuard.SnapshotSource} on the connector's main JDBC connection. Every method ends the
 * transaction it ran in before it returns, whether the connection runs with autocommit or not (it can be either after
 * a reconnect), so each snapshot is new and the connection is never left idle in a transaction.
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

    private final PostgresConnection connection;
    private final Predicate<TableId> capturedTable;
    private Boolean modern;

    public JdbcCommitVisibilitySnapshotSource(PostgresConnection connection, Predicate<TableId> capturedTable) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.capturedTable = Objects.requireNonNull(capturedTable, "capturedTable");
    }

    @Override
    public PostgresCommitVisibilityGuard.ServerState currentState() throws SQLException {
        return inOwnTransaction(() -> connection.queryAndMap(modern() ? STATE : STATE_BEFORE_13,
                connection.singleResultMapper(rs -> new PostgresCommitVisibilityGuard.ServerState(rs.getBoolean(1), PgSnapshot.valueOf(rs.getString(2))),
                        "Unable to read the current snapshot")));
    }

    @Override
    public Set<Long> runningWritersOfCapturedTables() throws SQLException {
        return inOwnTransaction(() -> connection.queryAndMap(RUNNING_WRITERS, rs -> {
            final Set<Long> writers = new HashSet<>();
            while (rs.next()) {
                if (capturedTable.test(new TableId(null, rs.getString(2), rs.getString(3)))) {
                    writers.add(rs.getLong(1));
                }
            }
            return writers;
        }));
    }

    @FunctionalInterface
    private interface Query<T> {
        T run() throws SQLException;
    }

    private <T> T inOwnTransaction(Query<T> query) throws SQLException {
        final Connection jdbc = connection.connection();
        if (jdbc.getAutoCommit()) {
            return query.run();
        }
        // End what a previous statement left open, so the query starts a new transaction, and end the query's own.
        jdbc.commit();
        final T result;
        try {
            result = query.run();
        }
        catch (SQLException | RuntimeException e) {
            try {
                jdbc.rollback();
            }
            catch (SQLException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        jdbc.commit();
        return result;
    }

    private boolean modern() throws SQLException {
        if (modern == null) {
            modern = connection.connection().getMetaData().getDatabaseMajorVersion() >= 13;
        }
        return modern;
    }
}
