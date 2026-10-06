/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;

public class PostgresReadOnlyIncrementalSnapshotContextTest {

    private final PostgresReadOnlyIncrementalSnapshotContext<Object> context = new PostgresReadOnlyIncrementalSnapshotContext<>();

    @Test
    void aWindowIsReleasedOnlyOnceTheStreamReachesItsReleasePosition() {
        assertThat(context.releaseDue(Lsn.valueOf("0/2000"))).as("no chunk read").isFalse();

        context.chunkRead(Lsn.valueOf("0/1000"));
        assertThat(context.isWindowOpen()).isTrue();
        assertThat(context.deduplicationNeeded()).isTrue();

        assertThat(context.releaseDue(Lsn.valueOf("0/FFF"))).isFalse();
        assertThat(context.releaseDue(null)).as("position unknown").isFalse();
        assertThat(context.releaseDue(Lsn.valueOf("0/1000"))).as("at the release position").isTrue();
        assertThat(context.releaseDue(Lsn.valueOf("1/0"))).isTrue();

        context.releaseWindow();
        assertThat(context.isWindowOpen()).isFalse();
        assertThat(context.releaseDue(Lsn.valueOf("1/0"))).isFalse();
        assertThat(context.getReleasePosition()).isNull();
    }
}
