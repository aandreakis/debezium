/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresCommitVisibilityGuard.Mode;
import io.debezium.connector.postgresql.PostgresCommitVisibilityGuard.ServerState;

public class PostgresCommitVisibilityGuardTest {

    private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long EPOCH = 1L << 32;

    private final FakeSource source = new FakeSource();
    private final FakeClock clock = new FakeClock();
    private final PostgresCommitVisibilityGuard guard = new PostgresCommitVisibilityGuard(source, Duration.ofSeconds(5), clock::now, clock::sleep);

    @Test
    void streamIdsMapToTheFullIdNearestTheSnapshotsXmax() {
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(1000, 995)).isEqualTo(995);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(1000, 1003)).as("at or above xmax").isEqualTo(1003);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(EPOCH + 10, 0xFFFF_FFF0L)).as("previous epoch").isEqualTo(EPOCH - 16);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(EPOCH - 10, 5)).as("next epoch").isEqualTo(EPOCH + 5);
        assertThat(PostgresCommitVisibilityGuard.toFullTransactionId(3L * EPOCH + 7, 7)).isEqualTo(3L * EPOCH + 7);
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
    void startOnAPrimaryTracksTheRunningWritersOfCapturedTablesThatAreNotVisible() {
        source.idle = "990:1000:991,995,997";
        source.writers = Set.of(991L, 995L, 999L);

        guard.start();

        assertThat(guard.mode()).isEqualTo(Mode.PRIMARY);
        assertThat(source.calls).containsExactly("state", "writers");
        assertThat(guard.trackedIds()).as("997 wrote no captured table; 999 is finished in the snapshot").containsExactlyInAnyOrder(991L, 995L);
    }

    @Test
    void theCaptureMapsWriterIdsAcrossAnEpoch() {
        source.idle = (EPOCH - 5) + ":" + (EPOCH + 3) + ":" + (EPOCH - 5) + "," + (EPOCH + 1);
        source.writers = Set.of(0xFFFF_FFFBL, 1L);

        guard.start();

        assertThat(guard.trackedIds()).containsExactlyInAnyOrder(EPOCH - 5, EPOCH + 1);
    }

    @Test
    void onAStandbyNothingIsTrackedAndEveryCheckPasses() throws Exception {
        source.inRecovery = true;
        source.idle = "100:110:105";
        source.writers = Set.of(105L);

        guard.start();
        guard.recordEmitted(106L);

        assertThat(guard.mode()).isEqualTo(Mode.STANDBY);
        assertThat(guard.unconfirmedCount()).isZero();
        assertThat(guard.awaitVisible()).isTrue();
        assertThat(source.calls).doesNotContain("writers");
    }

    @Test
    void afterAPromotionTheNextCheckCapturesAndWaits() throws Exception {
        source.inRecovery = true;
        source.idle = "100:110:";
        guard.start();
        assertThat(guard.awaitVisible()).isTrue();

        source.inRecovery = false;
        source.writers = Set.of(203L);
        source.snapshots("200:210:203", "200:210:203", "200:210:203");
        source.idle = "204:210:";
        source.calls.clear();

        assertThat(guard.awaitVisible()).isTrue();

        assertThat(guard.mode()).isEqualTo(Mode.PRIMARY);
        assertThat(source.calls).startsWith("state", "writers", "state");
        assertThat(clock.sleeps).as("waited for the captured writer").isNotEmpty();
    }

    @Test
    void aTransactionIsRecordedOncePerCommitAlsoWhenItsRawIdRepeats() {
        source.idle = "100:200:";
        guard.start();

        guard.recordEmitted(150L);
        guard.recordEmitted(150L);
        assertThat(guard.trackedIds()).containsExactly(150L);

        guard.transactionCommitted();
        guard.recordEmitted(150L);

        assertThat(guard.trackedIds()).as("the same raw id after a commit is another transaction").containsExactly(150L, 150L);
    }

    @Test
    void streamIdsAreMappedAgainstASnapshotAtMostTenSecondsOld() {
        source.idle = (EPOCH - 100) + ":" + (EPOCH + 5) + ":";
        guard.start();
        guard.recordEmitted(0xFFFF_FFF0L);
        assertThat(guard.trackedIds()).containsExactly(EPOCH - 16);

        clock.advance(11 * SECOND);
        source.idle = (2 * EPOCH - 3) + ":" + (2 * EPOCH + 8) + ":" + (2 * EPOCH - 3);
        source.calls.clear();
        guard.transactionCommitted();
        guard.recordEmitted(3L);

        assertThat(source.calls).as("the old reference was refreshed first").containsExactly("state");
        assertThat(guard.trackedIds()).as("the first transaction was pruned with the new snapshot").containsExactly(2 * EPOCH + 3);
    }

    @Test
    void aFinishedTransactionRecordedLongAgoNeverMapsToAFutureId() throws Exception {
        source.idle = "100:110:";
        guard.start();
        guard.recordEmitted(101L);

        clock.advance(SECOND);
        source.idle = "2147483750:2147483750:";

        assertThat(guard.awaitVisible()).as("101 is below xmin: it was mapped when it was recorded").isTrue();
    }

    @Test
    void aFullSixtyFourBitIdIsTrackedAsIs() {
        source.idle = "100:200:";
        guard.start();

        guard.recordEmitted(EPOCH + 7);

        assertThat(guard.trackedIds()).containsExactly(EPOCH + 7);
    }

    @Test
    void theCheckPassesWithoutPollingWhenEveryTrackedTransactionFinished() throws Exception {
        source.idle = "100:200:150";
        guard.start();
        guard.recordEmitted(150L);
        source.idle = "151:200:";

        assertThat(guard.awaitVisible()).isTrue();
        assertThat(clock.sleeps).isEmpty();
    }

    @Test
    void theCheckPollsUntilTheTrackedTransactionsAreVisible() throws Exception {
        source.idle = "100:200:150";
        guard.start();
        guard.recordEmitted(150L);
        source.snapshots("100:200:150", "100:200:150", "151:200:");

        assertThat(guard.awaitVisible()).isTrue();
        assertThat(clock.sleeps).containsExactly(MS, 2 * MS);
    }

    @Test
    void theCheckFailsWhenTheWaitExceedsMaxWait() throws Exception {
        source.idle = "100:200:150";
        guard.start();
        guard.recordEmitted(150L);

        assertThat(guard.awaitVisible()).isFalse();
        assertThat(clock.slept()).isEqualTo(5 * SECOND);
        assertThat(guard.consecutiveFailedChecks()).isEqualTo(1);
        assertThat(guard.trackedIds()).containsExactly(150L);
    }

    @Test
    void aTransactionPendingLongerThanMaxWaitFailsTheCheckAtOnce() throws Exception {
        source.idle = "100:200:150";
        guard.start();
        guard.recordEmitted(150L);
        clock.advance(6 * SECOND);

        assertThat(guard.awaitVisible()).isFalse();
        assertThat(clock.sleeps).isEmpty();

        source.idle = "151:200:";
        assertThat(guard.awaitVisible()).as("passes once it finished").isTrue();
        assertThat(guard.consecutiveFailedChecks()).isZero();
    }

    @Test
    void maintainPrunesAtMostOnceASecondAndOnlyWhileTransactionsAreTracked() {
        source.idle = "100:200:";
        guard.start();
        source.calls.clear();

        guard.maintain();
        assertThat(source.calls).as("nothing tracked").isEmpty();

        guard.recordEmitted(150L);
        clock.advance(SECOND / 2);
        guard.maintain();
        assertThat(source.calls).isEmpty();

        clock.advance(SECOND);
        source.idle = "151:200:";
        guard.maintain();
        assertThat(source.calls).containsExactly("state");
        assertThat(guard.unconfirmedCount()).isZero();
    }

    @Test
    void aFailedQueryLosesTrackingAndTheNextCheckCapturesAgain() throws Exception {
        source.idle = "100:200:150";
        guard.start();
        guard.recordEmitted(150L);
        clock.advance(2 * SECOND);
        source.failNextState = new SQLException("connection lost");

        guard.maintain();

        assertThat(guard.mode()).isEqualTo(Mode.UNKNOWN);
        assertThat(guard.unconfirmedCount()).isZero();
        guard.transactionCommitted();
        guard.recordEmitted(160L);
        assertThat(guard.unconfirmedCount()).as("nothing is tracked until the capture").isZero();

        source.writers = Set.of(150L, 160L);
        source.snapshots("100:200:150,160", "100:200:150,160", "100:200:150,160");
        source.idle = "161:200:";
        source.calls.clear();

        assertThat(guard.awaitVisible()).isTrue();
        assertThat(source.calls).startsWith("state", "writers", "state");
        assertThat(guard.mode()).isEqualTo(Mode.PRIMARY);
        assertThat(clock.sleeps).as("waited for both captured writers").isNotEmpty();
    }

    @Test
    void aFailedCheckRethrowsAndCapturesAgainNextTime() {
        source.idle = "100:200:";
        guard.start();
        source.failNextState = new SQLException("connection lost");

        assertThatThrownBy(guard::awaitVisible).isInstanceOf(SQLException.class);
        assertThat(guard.mode()).isEqualTo(Mode.UNKNOWN);
    }

    @Test
    void aFailedStartIsRetriedByTheNextCheck() throws Exception {
        source.failNextState = new SQLException("not yet connected");
        guard.start();
        assertThat(guard.mode()).isEqualTo(Mode.UNKNOWN);

        source.idle = "100:200:";
        assertThat(guard.awaitVisible()).isTrue();
        assertThat(guard.mode()).isEqualTo(Mode.PRIMARY);
    }

    @Test
    void aPrimaryThatReportsRecoveryIsTreatedAsAFailedQuery() {
        source.idle = "100:200:";
        guard.start();
        source.inRecovery = true;

        assertThatThrownBy(guard::awaitVisible).isInstanceOf(IllegalStateException.class);
        assertThat(guard.mode()).isEqualTo(Mode.UNKNOWN);
    }

    @Test
    void manyFinishedTransactionsArePrunedInOnePass() throws Exception {
        source.idle = "100:200:";
        guard.start();
        for (long id = 1_000; id < 101_000; id++) {
            guard.recordEmitted(id);
            guard.transactionCommitted();
        }
        source.idle = "200000:200000:";

        assertThat(guard.awaitVisible()).isTrue();
        assertThat(guard.unconfirmedCount()).isZero();
    }

    static final class FakeSource implements PostgresCommitVisibilityGuard.SnapshotSource {
        final List<String> calls = new ArrayList<>();
        final Deque<String> queued = new ArrayDeque<>();
        String idle = "1:1:";
        boolean inRecovery;
        Set<Long> writers = Set.of();
        SQLException failNextState;

        void snapshots(String... snapshots) {
            queued.addAll(List.of(snapshots));
        }

        @Override
        public ServerState currentState() throws SQLException {
            calls.add("state");
            if (failNextState != null) {
                final SQLException failure = failNextState;
                failNextState = null;
                throw failure;
            }
            final String snapshot = queued.isEmpty() ? idle : queued.poll();
            return new ServerState(inRecovery, PgSnapshot.valueOf(snapshot));
        }

        @Override
        public Set<Long> runningWritersOfCapturedTables() {
            calls.add("writers");
            return writers;
        }
    }

    static final class FakeClock {
        private long nanos = 1_000 * SECOND;
        final List<Long> sleeps = new ArrayList<>();

        long now() {
            return nanos;
        }

        void sleep(long duration) {
            sleeps.add(duration);
            nanos += duration;
        }

        void advance(long duration) {
            nanos += duration;
        }

        long slept() {
            return sleeps.stream().mapToLong(Long::longValue).sum();
        }
    }
}
