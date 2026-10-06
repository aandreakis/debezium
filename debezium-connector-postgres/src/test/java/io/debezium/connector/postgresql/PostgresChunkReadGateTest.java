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
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

public class PostgresChunkReadGateTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private final AtomicLong clock = new AtomicLong(-5 * SECOND);
    private final ScriptedGuard guard = new ScriptedGuard();
    private final PostgresChunkReadGate gate = new PostgresChunkReadGate(guard, clock::get);

    @Test
    void inPlaceWaitIsHalfTheSemaphoreWaitBetweenOneHundredMillisecondsAndFiveSeconds() {
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofSeconds(10))).isEqualTo(Duration.ofSeconds(5));
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofSeconds(60))).isEqualTo(Duration.ofSeconds(5));
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofSeconds(2))).isEqualTo(Duration.ofSeconds(1));
        assertThat(PostgresChunkReadGate.inPlaceWait(Duration.ofMillis(50))).isEqualTo(Duration.ofMillis(100));
        assertThat(PostgresChunkReadGate.inPlaceWait(null)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void aFailedCheckPutsTheReadOffWithADelayThatDoublesToThirtySecondsAndResetsAfterAPass() {
        long[] expectedDelays = { 1, 2, 4, 8, 16, 30, 30 };
        for (long delaySeconds : expectedDelays) {
            guard.results.add(false);
            assertThat(gate.readyForChunkRead(null)).isFalse();
            assertThat(gate.isRetryPending()).isTrue();
            long failedAt = clock.get();
            clock.set(failedAt + delaySeconds * SECOND - 1);
            assertThat(gate.retryDue()).as("not before %d s", delaySeconds).isFalse();
            clock.set(failedAt + delaySeconds * SECOND);
            assertThat(gate.retryDue()).isTrue();
            assertThat(gate.isRetryPending()).isFalse();
        }

        guard.results.add(true);
        assertThat(gate.readyForChunkRead(null)).isTrue();
        guard.results.add(false);
        assertThat(gate.readyForChunkRead(null)).isFalse();
        clock.addAndGet(SECOND);
        assertThat(gate.retryDue()).as("the delay restarted at 1 s").isTrue();
    }

    @Test
    void aPassingCheckClearsAPutOffReadSoItIsNotReadAgain() {
        guard.results.add(false);
        assertThat(gate.readyForChunkRead(null)).isFalse();
        assertThat(gate.isRetryPending()).isTrue();

        // A resume signal reads before the retry is due, and the check passes.
        guard.results.add(true);
        assertThat(gate.readyForChunkRead(null)).isTrue();

        clock.addAndGet(60 * SECOND);
        assertThat(gate.isRetryPending()).isFalse();
        assertThat(gate.retryDue()).as("no second read at the next commit").isFalse();
    }

    @Test
    void aFailingQueryPutsTheReadOff() {
        guard.failure = new SQLException("connection lost");

        assertThat(gate.readyForChunkRead(null)).isFalse();
        assertThat(gate.isRetryPending()).isTrue();
    }

    @Test
    void anInterruptPutsTheReadOffAndKeepsTheInterruptFlag() {
        guard.interrupt = true;
        try {
            assertThat(gate.readyForChunkRead(null)).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void retryLaterPutsTheNextReadOffWithTheCurrentDelay() {
        gate.retryLater();

        assertThat(gate.retryDue()).isFalse();
        clock.addAndGet(SECOND);
        assertThat(gate.retryDue()).isTrue();
    }

    @Test
    void workQueuedOnAnotherThreadRunsInArrivalOrderOnTheBoundThread() throws Exception {
        gate.start();
        assertThat(guard.started).isTrue();
        List<String> ran = new ArrayList<>();
        AtomicBoolean otherIsBound = new AtomicBoolean(true);
        Thread other = new Thread(() -> {
            otherIsBound.set(gate.onEventThread());
            gate.queue("start", () -> ran.add("start"));
            gate.queue("window close", () -> ran.add("close"));
            gate.queue("pause", () -> ran.add("pause"));
        });
        other.start();
        other.join();

        assertThat(otherIsBound).isFalse();
        assertThat(gate.onEventThread()).isTrue();
        assertThat(ran).as("nothing runs on the other thread").isEmpty();

        gate.runQueued();

        assertThat(ran).containsExactly("start", "close", "pause");
        gate.runQueued();
        assertThat(ran).hasSize(3);
    }

    @Test
    void aControlOnTheStreamsThreadRunsAfterTheControlsQueuedBeforeIt() throws Exception {
        gate.start();
        List<String> ran = new ArrayList<>();
        Thread other = new Thread(() -> {
            try {
                gate.control("pause", () -> ran.add("pause"));
                gate.control("start", () -> ran.add("start"));
            }
            catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        other.start();
        other.join();
        assertThat(ran).as("controls from another thread wait for the stream's thread").isEmpty();

        gate.control("resume", () -> ran.add("resume"));
        gate.control("stop", () -> ran.add("stop"));

        assertThat(ran).containsExactly("pause", "start", "resume", "stop");
    }

    @Test
    void aFailingRequestIsSkippedAndTheNextOneRuns() throws Exception {
        gate.start();
        List<String> ran = new ArrayList<>();
        gate.queue("start", () -> {
            throw new IllegalArgumentException("bad data collection pattern");
        });
        gate.queue("stop", () -> ran.add("stop"));

        gate.runQueued();

        assertThat(ran).containsExactly("stop");
        assertThat(gate.isRetryPending()).as("a chunk read is retried later").isTrue();
    }

    @Test
    void anInterruptRaisedByARequestStopsThePassAndLeavesTheRestQueued() throws Exception {
        gate.start();
        List<String> ran = new ArrayList<>();
        gate.queue("start", () -> {
            ran.add("start");
            // As the visibility check does when its wait is interrupted: the flag is kept, nothing is thrown.
            Thread.currentThread().interrupt();
        });
        gate.queue("stop", () -> ran.add("stop"));
        try {
            assertThatThrownBy(gate::runQueued).isInstanceOf(InterruptedException.class);
            assertThat(ran).containsExactly("start");
        }
        finally {
            Thread.interrupted();
        }

        gate.runQueued();
        assertThat(ran).containsExactly("start", "stop");
    }

    @Test
    void anInterruptStopsTheQueueAndLeavesTheRestQueued() throws Exception {
        gate.start();
        List<String> ran = new ArrayList<>();
        gate.queue("start", () -> {
            throw new InterruptedException();
        });
        gate.queue("stop", () -> ran.add("stop"));

        assertThatThrownBy(gate::runQueued).isInstanceOf(InterruptedException.class);
        assertThat(ran).isEmpty();

        gate.runQueued();
        assertThat(ran).containsExactly("stop");
    }

    @Test
    void aControlIssuedByRunningWorkRunsAfterThatWork() throws Exception {
        gate.start();
        List<String> ran = new ArrayList<>();
        gate.control("resume", () -> {
            ran.add("resume begins");
            gate.control("window close", () -> ran.add("window close"));
            ran.add("resume ends");
        });

        assertThat(ran).containsExactly("resume begins", "resume ends", "window close");
    }

    @Test
    void aReadThatLeftTheSnapshotWithoutAWindowIsRetried() {
        gate.afterChunkRead(true);

        assertThat(gate.isRetryPending()).isTrue();
        assertThat(gate.retryDue()).isFalse();
        clock.addAndGet(SECOND);
        assertThat(gate.retryDue()).isTrue();

        gate.afterChunkRead(false);
        assertThat(gate.isRetryPending()).as("a read with a window needs no retry").isFalse();
    }

    @Test
    void aPutOffReadKeepsItsDelayAfterTheRead() {
        guard.results.add(false);
        assertThat(gate.readyForChunkRead(null)).isFalse();

        gate.afterChunkRead(true);

        clock.addAndGet(SECOND - 1);
        assertThat(gate.retryDue()).isFalse();
        clock.addAndGet(1);
        assertThat(gate.retryDue()).isTrue();
    }

    @Test
    void anUnboundGateTreatsEveryThreadAsTheStreamsThread() {
        assertThat(gate.onEventThread()).isTrue();
    }

    @Test
    void theEndOfASnapshotDropsAPutOffRead() {
        guard.results.add(false);
        assertThat(gate.readyForChunkRead(null)).isFalse();

        gate.snapshotEnded();

        assertThat(gate.isRetryPending()).isFalse();
        assertThat(gate.retryDue()).isFalse();
    }

    /** A guard on a source that reports an idle primary; tests override what they need. */
    static class ScriptedGuardBase extends PostgresCommitVisibilityGuard {
        ScriptedGuardBase() {
            super(new SnapshotSource() {
                @Override
                public ServerState currentState() {
                    return new ServerState(false, PgSnapshot.valueOf("1:1:"));
                }

                @Override
                public Map<Long, Set<TableId>> runningWriters() {
                    return Map.of();
                }

            }, Duration.ofSeconds(5));
        }
    }

    /** A guard whose checks return scripted results. */
    static final class ScriptedGuard extends ScriptedGuardBase {
        final Deque<Boolean> results = new ArrayDeque<>();
        SQLException failure;
        boolean interrupt;
        boolean started;

        @Override
        public void start() {
            started = true;
        }

        @Override
        public boolean awaitVisible(Collection<TableId> readTables) throws SQLException, InterruptedException {
            if (failure != null) {
                throw failure;
            }
            if (interrupt) {
                throw new InterruptedException();
            }
            return results.isEmpty() || results.poll();
        }
    }
}
