/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;

import io.debezium.connector.postgresql.connection.PostgresConnection;

/**
 * {@link PostgresCommitVisibilityGuard.SnapshotSource} on the connector's main JDBC connection, which runs with
 * autocommit off. Every method first ends any transaction open on the connection, so each snapshot is taken in a new
 * transaction and is new whatever the session's isolation level; the transaction a method leaves open is ended by
 * the next one, or by the commit {@code readChunk} runs before it opens the window.
 */
public class JdbcCommitVisibilitySnapshotSource implements PostgresCommitVisibilityGuard.SnapshotSource {

    private final PostgresConnection connection;

    public JdbcCommitVisibilitySnapshotSource(PostgresConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean isInRecovery() throws SQLException {
        connection.commit();
        return connection.queryAndMap("SELECT pg_is_in_recovery()", rs -> rs.next() && rs.getBoolean(1));
    }

    @Override
    public void assignAndEndTransactionId() throws SQLException {
        connection.commit();
        connection.query(modern() ? "SELECT pg_current_xact_id()" : "SELECT txid_current()", rs -> {
        });
        // Ending the transaction, by commit or rollback, moves the server's latest completed transaction id past the
        // id just assigned. A rollback writes no commit record, so it never waits for a synchronous standby.
        connection.rollback();
    }

    @Override
    public String currentSnapshot() throws SQLException {
        connection.commit();
        return connection.queryAndMap(modern() ? "SELECT pg_current_snapshot()::text" : "SELECT txid_current_snapshot()::text",
                connection.singleResultMapper(rs -> rs.getString(1), "Unable to read the current snapshot"));
    }

    private boolean modern() throws SQLException {
        return connection.connection().getMetaData().getDatabaseMajorVersion() >= 13;
    }
}
