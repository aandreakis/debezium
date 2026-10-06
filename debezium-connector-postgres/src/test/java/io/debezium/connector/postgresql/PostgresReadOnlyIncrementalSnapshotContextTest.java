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
    void aWindowIsReleasedOnceTheStreamReachesItsReleasePosition() {
        assertThat(context.releaseDueAtCommit(Lsn.valueOf("0/2000"))).as("no chunk read").isFalse();
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("0/2000"))).as("no chunk read").isFalse();

        context.chunkRead(Lsn.valueOf("0/1000"), false);
        assertThat(context.isWindowOpen()).isTrue();
        assertThat(context.deduplicationNeeded()).isTrue();

        assertThat(context.releaseDueAtCommit(Lsn.valueOf("0/FFF"))).isFalse();
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("0/FFF"))).isFalse();
        assertThat(context.releaseDueAtCommit(null)).as("position unknown").isFalse();
        assertThat(context.releaseDueWhenIdle(null)).as("position unknown").isFalse();
        assertThat(context.releaseDueWhenIdle(Lsn.INVALID_LSN)).as("position unknown").isFalse();
        assertThat(context.releaseDueAtCommit(Lsn.valueOf("0/1000"))).as("at the release position").isTrue();
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("0/1000"))).as("at the release position").isTrue();
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("1/0"))).isTrue();

        context.releaseWindow();
        assertThat(context.isWindowOpen()).isFalse();
        assertThat(context.releaseDueAtCommit(Lsn.valueOf("1/0"))).isFalse();
        assertThat(context.getReleasePosition()).isNull();
    }

    @Test
    void aStandbysReplayPositionMustBePassedNotJustReached() {
        context.chunkRead(Lsn.valueOf("0/1000"), true);

        assertThat(context.releaseDueAtCommit(Lsn.valueOf("0/1000"))).as("a commit ending at the replay position came before the record in replay").isFalse();
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("0/1000"))).isFalse();
        assertThat(context.releaseDueAtCommit(Lsn.valueOf("0/1001"))).isTrue();
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("0/1001"))).isTrue();

        context.releaseWindow();
        context.chunkRead(Lsn.valueOf("0/2000"), false);
        assertThat(context.releaseDueWhenIdle(Lsn.valueOf("0/2000"))).as("the next window reaches again").isTrue();
    }

    @Test
    void aStreamAtAReleasePositionItMustPassWaitsThere() {
        context.chunkRead(Lsn.valueOf("0/1000"), true);

        assertThat(context.waitsAtReleasePosition(Lsn.valueOf("0/FFF"))).isFalse();
        assertThat(context.waitsAtReleasePosition(Lsn.valueOf("0/1000"))).isTrue();
        assertThat(context.waitsAtReleasePosition(null)).isFalse();
        assertThat(context.waitsAtReleasePosition(Lsn.INVALID_LSN)).isFalse();

        context.releaseWindow();
        context.chunkRead(Lsn.valueOf("0/2000"), false);
        assertThat(context.waitsAtReleasePosition(Lsn.valueOf("0/2000"))).as("a position to reach").isFalse();
    }
}
