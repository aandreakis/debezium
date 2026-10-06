/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

public class PostgresChunkReadGateTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    /** A transaction that stays invisible until {@link #visible} is set. */
    private final AtomicBoolean visible = new AtomicBoolean(false);
    private final AtomicLong clock = new AtomicLong(-5 * SECOND);
    private final PostgresCommitVisibilityGuard.SnapshotSource source = new PostgresCommitVisibilityGuard.SnapshotSource() {
        @Override
        public boolean isInRecovery() {
            return false;
        }

        @Override
        public void assignAndEndTransactionId() {
        }

        @Override
        public String currentSnapshot() {
            return visible.get() ? "2000:2000:" : "1000:2000:1000";
        }
    };
    private final PostgresCommitVisibilityGuard guard = new PostgresCommitVisibilityGuard(source, Duration.ofSeconds(5), clock::get,
            nanos -> clock.addAndGet(nanos));
    private final PostgresChunkReadGate gate = new PostgresChunkReadGate(guard, clock::get);

    @Test
    void inPlaceWaitIsHalfTheSemaphoreWaitBetweenOneHundredMillisecondsAndFiveSeconds() {
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofSeconds(10))).isEqualTo(Duration.ofSeconds(5));
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofSeconds(60))).isEqualTo(Duration.ofSeconds(5));
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofSeconds(2))).isEqualTo(Duration.ofSeconds(1));
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofMillis(50))).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void aFailedCheckPutsTheReadOffWithADelayThatDoublesToThirtySecondsAndResetsAfterAPass() {
        long[] expectedDelays = { 1, 2, 4, 8, 16, 30, 30 };
        for (long delaySeconds : expectedDelays) {
            assertThat(gate.readyForChunkRead()).isFalse();
            assertThat(gate.isReadPending()).isTrue();
            long failedAt = clock.get();
            clock.set(failedAt + delaySeconds * SECOND - 1);
            assertThat(gate.retryDue()).as("not before %d s", delaySeconds).isFalse();
            clock.set(failedAt + delaySeconds * SECOND);
            assertThat(gate.retryDue()).isTrue();
            assertThat(gate.isReadPending()).isFalse();
        }

        visible.set(true);
        assertThat(gate.readyForChunkRead()).isTrue();
        visible.set(false);
        guard.recordEmitted(1000L);
        assertThat(gate.readyForChunkRead()).isFalse();
        long failedAt = clock.get();
        clock.set(failedAt + SECOND);
        assertThat(gate.retryDue()).as("the delay restarted at 1 s").isTrue();
    }

    @Test
    void aReadRequestedOnAnotherThreadRunsAtTheNextOpportunityOnTheBoundThread() throws Exception {
        gate.bindEventThread();
        AtomicBoolean onBound = new AtomicBoolean(true);
        Thread other = new Thread(() -> {
            onBound.set(gate.onEventThread());
            gate.deferReadToEventThread();
            gate.deferCloseToEventThread("chunk-1-close");
        });
        other.start();
        other.join();

        assertThat(onBound).isFalse();
        assertThat(gate.onEventThread()).isTrue();
        assertThat(gate.retryDue()).as("no backoff: due at once").isTrue();
        assertThat(gate.takePendingClose()).isEqualTo("chunk-1-close");
        assertThat(gate.takePendingClose()).isNull();
    }

    @Test
    void theEndOfASnapshotDropsAPutOffReadAndTheTrackedTransactions() {
        assertThat(gate.readyForChunkRead()).isFalse();
        assertThat(guard.isTracking()).isTrue();

        gate.snapshotEnded();

        assertThat(gate.isReadPending()).isFalse();
        assertThat(gate.retryDue()).isFalse();
        assertThat(guard.isTracking()).isFalse();
        assertThat(guard.unconfirmedCount()).isZero();
    }
}
