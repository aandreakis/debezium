/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Decides when the PostgreSQL incremental snapshot may read a chunk.
 * <ul>
 * <li>All work of the incremental snapshot runs on the thread that processes the replication stream (the thread that
 * started the incremental snapshot source), so the set of transactions whose changes were emitted is fixed during a
 * read and the snapshot's state has a single writer. A signal handled on another thread, for example from Kafka, JMX
 * or a file, is queued and runs on that thread at its next transaction commit or idle poll, in arrival order.</li>
 * <li>A chunk is read only when {@link PostgresCommitVisibilityGuard} confirms that every transaction whose changes
 * were emitted is visible to new snapshots. Otherwise the read is put off and retried at a commit or idle poll after
 * 1 s, doubling to 30 s while checks keep failing; any passing check clears the put-off read and resets the delay.
 * Streaming continues in between.</li>
 * </ul>
 */
public class PostgresChunkReadGate {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChunkReadGate.class);

    static final long INITIAL_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long MAX_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(30);
    static final Duration MAX_IN_PLACE_WAIT = Duration.ofSeconds(5);
    static final Duration MIN_IN_PLACE_WAIT = Duration.ofMillis(100);

    /**
     * Snapshot work handed to the replication stream's thread.
     */
    @FunctionalInterface
    public interface Work {
        void run() throws InterruptedException;
    }

    private final PostgresCommitVisibilityGuard guard;
    private final LongSupplier nanoClock;
    private final ConcurrentLinkedQueue<Work> queued = new ConcurrentLinkedQueue<>();

    private volatile Thread eventThread;
    private boolean retryPending;
    private long retryNotBeforeNanos;
    private long retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;

    public PostgresChunkReadGate(PostgresCommitVisibilityGuard guard) {
        this(guard, System::nanoTime);
    }

    PostgresChunkReadGate(PostgresCommitVisibilityGuard guard, LongSupplier nanoClock) {
        this.guard = Objects.requireNonNull(guard, "guard");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /**
     * The gate of an incremental snapshot source on the connector's main connection: it tracks transactions on the
     * tables the connector captures.
     */
    static PostgresChunkReadGate create(RelationalDatabaseConnectorConfig config, PostgresConnection connection) {
        return new PostgresChunkReadGate(new PostgresCommitVisibilityGuard(
                new JdbcCommitVisibilitySnapshotSource(connection, tableId -> config.getTableFilters().dataCollectionFilter().isIncluded(tableId)),
                inPlaceWait(config.getSignalProcessorSemaphoreWait())));
    }

    /**
     * The longest a check waits in place, without processing events, before the read is put off. The wait runs on the
     * stream's thread, which may hold the signal processor's semaphore at that point, so it stays below half of
     * {@code signal.processor.semaphore.wait.ms}.
     */
    static Duration inPlaceWait(Duration signalSemaphoreWait) {
        if (signalSemaphoreWait == null) {
            return MAX_IN_PLACE_WAIT;
        }
        Duration half = signalSemaphoreWait.dividedBy(2);
        Duration wait = half.compareTo(MAX_IN_PLACE_WAIT) < 0 ? half : MAX_IN_PLACE_WAIT;
        return wait.compareTo(MIN_IN_PLACE_WAIT) < 0 ? MIN_IN_PLACE_WAIT : wait;
    }

    /**
     * Binds the gate to the calling thread, the one that starts the incremental snapshot source and then processes the
     * replication stream, and starts tracking emitted transactions.
     */
    public void start() {
        eventThread = Thread.currentThread();
        guard.start();
    }

    public boolean onEventThread() {
        final Thread bound = eventThread;
        return bound == null || bound == Thread.currentThread();
    }

    /**
     * Queues snapshot work requested on another thread; it runs on the stream's thread at its next commit or idle poll.
     */
    public void queue(String what, Work work) {
        queued.add(Objects.requireNonNull(work, "work"));
        LOGGER.info("Incremental snapshot {} requested outside the replication stream's thread; it runs there at the next commit or idle poll", what);
    }

    /**
     * Runs the queued snapshot work, in arrival order, on the stream's thread.
     */
    public void runQueued() throws InterruptedException {
        Work work;
        while ((work = queued.poll()) != null) {
            work.run();
        }
    }

    /**
     * Called right before a chunk read opens its window, on the stream's thread. Returns whether the read may run; if
     * not, the read is put off and {@link #retryDue()} reports when to try again. Never throws.
     */
    public boolean readyForChunkRead() {
        boolean visible;
        try {
            visible = guard.awaitVisible();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Interrupted while waiting for commit visibility; the chunk read is put off");
            visible = false;
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.warn("Commit-visibility check failed; the chunk read is put off and retried", e);
            visible = false;
        }
        if (visible) {
            retryPending = false;
            retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
            return true;
        }
        retryLater();
        return false;
    }

    /**
     * Puts the next chunk read off: it is retried at a commit or idle poll once the current delay has passed.
     */
    public void retryLater() {
        retryPending = true;
        retryNotBeforeNanos = nanoClock.getAsLong() + retryDelayNanos;
        retryDelayNanos = Math.min(retryDelayNanos * 2, MAX_RETRY_DELAY_NANOS);
    }

    /** Whether a put-off chunk read is due now; clears it when it is. */
    public boolean retryDue() {
        if (!retryPending || nanoClock.getAsLong() - retryNotBeforeNanos < 0) {
            return false;
        }
        retryPending = false;
        return true;
    }

    /** Records the transaction of an emitted change. */
    public void recordEmitted(OffsetContext offsetContext) {
        if (offsetContext instanceof PostgresOffsetContext) {
            guard.recordEmitted(((PostgresOffsetContext) offsetContext).transactionId());
        }
    }

    /** A transaction's commit was processed. */
    public void transactionCommitted() {
        guard.transactionCommitted();
    }

    /** At a commit or idle poll: keeps the tracked set small. */
    public void maintain() {
        guard.maintain();
    }

    /** The snapshot ended or stopped: forget a put-off read. Tracking goes on for the next snapshot. */
    public void snapshotEnded() {
        retryPending = false;
        retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
    }

    boolean isRetryPending() {
        return retryPending;
    }
}
