/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class PostgresCommitVisibilityGuardTest {

    private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private final FakeSource source = new FakeSource();
    private final FakeClock clock = new FakeClock();
    private final PostgresCommitVisibilityGuard guard = new PostgresCommitVisibilityGuard(source, Duration.ofSeconds(5), clock::now, clock::sleep);

    @Test
    void streamIdsMapToTheFullIdNearestTheSnapshotsXmax() {
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(1000, 995)).isEqualTo(995);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(1000, 1003)).as("at or above xmax").isEqualTo(1003);
        long epoch1 = 1L << 32;
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(epoch1 + 10, 0xFFFF_FFF0L)).as("previous epoch").isEqualTo(epoch1 - 16);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(epoch1 - 10, 5)).as("next epoch").isEqualTo(epoch1 + 5);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(3L * epoch1 + 7, 7)).isEqualTo(3L * epoch1 + 7);
    }

    @Test
    void aTransactionIsFinishedBelowXminOrBelowXmaxWhenNotRunning() {
        PgSnapshot snapshot = PgSnapshot.valueOf("100:110:100,105");
        assertThat(PostgresCommitVisibilityGuard.isFinished(snapshot, 99)).isTrue();
        assertThat(PostgresCommitVisibilityGuard.isFinished(snapshot, 100)).isFalse();
        assertThat(PostgresCommitVisibilityGuard.isFinished(snapshot, 104)).isTrue();
        assertThat(PostgresCommitVisibilityGuard.isFinished(snapshot, 105)).isFalse();
        assertThat(PostgresCommitVisibilityGuard.isFinished(snapshot, 110)).as("at xmax").isFalse();
    }

    @Test
    void theFirstCheckCapturesTheRunningTransactionsAfterEndingAFreshId() throws Exception {
        source.snapshots("990:1000:991,995");
        source.idle = "990:1000:991,995";

        assertThat(guard.awaitVisible()).isFalse();

        assertThat(source.calls).startsWith("recovery?", "assign-and-end", "snapshot");
        assertThat(guard.isTracking()).isTrue();
        assertThat(guard.trackedIds()).containsExactlyInAnyOrder(991L, 995L);
    }

    @Test
    void trackingRecordsEmittedTransactionsOnlyWhileASnapshotRunsAndOncePerTransaction() throws Exception {
        guard.recordEmitted(42L);
        assertThat(guard.unconfirmedCount()).as("not tracking yet").isZero();

        source.snapshots("50:50:");
        assertThat(guard.awaitVisible()).isTrue();
        guard.recordEmitted(51L);
        guard.recordEmitted(51L);
        guard.recordEmitted(52L);
        guard.recordEmitted(null);

        assertThat(guard.unconfirmedCount()).isEqualTo(2);
    }

    @Test
    void passesOnTheFirstSnapshotWithoutPolling() throws Exception {
        tracking();
        guard.recordEmitted(1001L);
        source.snapshots("1002:1002:");

        assertThat(guard.awaitVisible()).isTrue();
        assertThat(clock.sleeps).isEmpty();
        assertThat(guard.unconfirmedCount()).isZero();
    }

    @Test
    void pollsWithDoublingIntervalsCappedAtFiftyMillisecondsUntilVisible() throws Exception {
        tracking();
        guard.recordEmitted(1001L);
        source.snapshots("1001:1002:1001", "1001:1002:1001", "1001:1002:1001", "1001:1002:1001", "1001:1002:1001", "1001:1002:1001",
                "1001:1002:1001", "1002:1002:");

        assertThat(guard.awaitVisible()).isTrue();
        assertThat(clock.sleeps).containsExactly(MS, 2 * MS, 4 * MS, 8 * MS, 16 * MS, 32 * MS, 50 * MS);
    }

    @Test
    void consecutiveFailuresDoubleTheBudgetUntilTheEntryCountsAsStuck() throws Exception {
        tracking();
        guard.recordEmitted(1001L);
        source.idle = "1001:1002:1001";

        assertThat(failedCheckDuration()).isEqualTo(SECOND);
        assertThat(failedCheckDuration()).isEqualTo(2 * SECOND);
        assertThat(failedCheckDuration()).as("the entry is 3 s old, below the 5 s limit").isEqualTo(4 * SECOND);
        assertThat(failedCheckDuration()).as("the entry is 7 s old: stuck, no polling").isZero();
        assertThat(guard.consecutiveFailedChecks()).isEqualTo(4);
    }

    @Test
    void anEntryPendingLongerThanTheLimitFailsTheCheckAtOnce() throws Exception {
        tracking();
        guard.recordEmitted(1001L);
        source.idle = "1001:1002:1001";
        clock.now += 6 * SECOND;

        long before = clock.now;
        assertThat(guard.awaitVisible()).isFalse();
        assertThat(clock.now).as("no polling").isEqualTo(before);
        assertThat(guard.consecutiveFailedChecks()).isEqualTo(1);
    }

    @Test
    void aPassResetsTheFailureCount() throws Exception {
        tracking();
        guard.recordEmitted(1001L);
        source.idle = "1001:1002:1001";
        assertThat(guard.awaitVisible()).isFalse();
        assertThat(guard.consecutiveFailedChecks()).isEqualTo(1);

        source.idle = "1002:1002:";
        assertThat(guard.awaitVisible()).isTrue();
        assertThat(guard.consecutiveFailedChecks()).isZero();
    }

    @Test
    void entriesAreMappedAtTheFirstSnapshotAndComparedExactlyAfterwards() throws Exception {
        long epoch1 = 1L << 32;
        source.snapshots(epoch1 + ":" + epoch1 + ":");
        assertThat(guard.awaitVisible()).isTrue();
        guard.recordEmitted(0xFFFF_FFF0L);
        source.snapshots((epoch1 - 16) + ":" + (epoch1 + 1) + ":" + (epoch1 - 16));
        source.idle = (epoch1 - 16) + ":" + (epoch1 + 1) + ":" + (epoch1 - 16);

        assertThat(guard.awaitVisible()).isFalse();
        assertThat(guard.trackedIds()).containsExactly(epoch1 - 16);
    }

    @Test
    void onAStandbyTheGuardDisablesItself() throws Exception {
        source.inRecovery = true;

        assertThat(guard.awaitVisible()).isTrue();
        guard.recordEmitted(1001L);
        assertThat(guard.awaitVisible()).isTrue();

        assertThat(guard.isDisabled()).isTrue();
        assertThat(source.calls).containsExactly("recovery?");
        assertThat(guard.unconfirmedCount()).isZero();
    }

    @Test
    void stopTrackingForgetsEverythingAndTheNextCheckCapturesAgain() throws Exception {
        source.snapshots("1000:1000:");
        guard.awaitVisible();
        guard.recordEmitted(1001L);

        guard.stopTracking();
        assertThat(guard.isTracking()).isFalse();
        assertThat(guard.unconfirmedCount()).isZero();

        source.snapshots("1002:1002:");
        assertThat(guard.awaitVisible()).isTrue();
        assertThat(source.calls.stream().filter("assign-and-end"::equals).count()).isEqualTo(2);
    }

    @Test
    void pruneRunsOnlyAboveOneHundredThousandEntries() throws Exception {
        tracking();
        for (int i = 0; i < PostgresCommitVisibilityGuard.PRUNE_SIZE_THRESHOLD; i++) {
            guard.recordEmitted(2000L + i);
        }
        source.calls.clear();
        guard.pruneIfOversized();
        assertThat(source.calls).isEmpty();

        guard.recordEmitted(2000L + PostgresCommitVisibilityGuard.PRUNE_SIZE_THRESHOLD);
        long xmax = 2001L + PostgresCommitVisibilityGuard.PRUNE_SIZE_THRESHOLD;
        source.snapshots(xmax + ":" + xmax + ":");
        guard.pruneIfOversized();
        assertThat(source.calls).containsExactly("snapshot");
        assertThat(guard.unconfirmedCount()).isZero();
    }

    private void tracking() throws SQLException, InterruptedException {
        source.snapshots("1000:1000:");
        assertThat(guard.awaitVisible()).isTrue();
        source.calls.clear();
    }

    private long failedCheckDuration() throws Exception {
        long before = clock.now;
        assertThat(guard.awaitVisible()).isFalse();
        return clock.now - before;
    }

    private static final class FakeSource implements PostgresCommitVisibilityGuard.SnapshotSource {
        private final List<String> calls = new ArrayList<>();
        private final Deque<String> queued = new ArrayDeque<>();
        private String idle = "1000:1000:";
        private boolean inRecovery;

        void snapshots(String... texts) {
            queued.addAll(List.of(texts));
        }

        @Override
        public boolean isInRecovery() {
            calls.add("recovery?");
            return inRecovery;
        }

        @Override
        public void assignAndEndTransactionId() {
            calls.add("assign-and-end");
        }

        @Override
        public String currentSnapshot() {
            calls.add("snapshot");
            return queued.isEmpty() ? idle : queued.removeFirst();
        }
    }

    /** Monotonic fake clock; sleeping advances it. */
    private static final class FakeClock {
        private long now = 1_000 * SECOND;
        private final List<Long> sleeps = new ArrayList<>();

        long now() {
            return now;
        }

        void sleep(long nanos) {
            sleeps.add(nanos);
            now += nanos;
        }
    }
}
