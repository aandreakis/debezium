/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;

/**
 * Decides when the PostgreSQL incremental snapshot may read a chunk.
 * <ul>
 * <li>A chunk is read only on the thread that processes the replication stream (the thread that started the
 * incremental snapshot source), so the set of transactions whose changes were emitted is fixed during the read. A
 * read or a window close requested on another thread, for example by a Kafka, JMX or file signal, is handed to that
 * thread, which runs it at its next transaction commit or idle heartbeat.</li>
 * <li>A chunk is read only when {@link PostgresCommitVisibilityGuard} confirms that every transaction whose changes
 * were emitted is visible to new snapshots. Otherwise the read is put off and retried at a commit or heartbeat after
 * 1 s, doubling to 30 s while checks keep failing, and the delay resets after a passing check. Streaming continues in
 * between.</li>
 * </ul>
 */
public class PostgresChunkReadGate {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChunkReadGate.class);

    static final long INITIAL_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long MAX_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(30);
    static final Duration MAX_IN_PLACE_WAIT = Duration.ofSeconds(5);
    static final Duration MIN_IN_PLACE_WAIT = Duration.ofMillis(100);

    private final PostgresCommitVisibilityGuard guard;
    private final LongSupplier nanoClock;

    private volatile Thread eventThread;
    private volatile boolean readPending;
    private volatile String pendingCloseId;
    private boolean backoffActive;
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
     * replication stream.
     */
    public void bindEventThread() {
        eventThread = Thread.currentThread();
    }

    public boolean onEventThread() {
        final Thread bound = eventThread;
        return bound == null || bound == Thread.currentThread();
    }

    /** A chunk read was requested on another thread: run it on the stream's thread at its next opportunity. */
    public void deferReadToEventThread() {
        readPending = true;
        LOGGER.info("Incremental snapshot chunk read requested outside the replication stream's thread; it runs at the next commit or heartbeat");
    }

    /** A window close arrived on another thread: run it on the stream's thread at its next opportunity. */
    public void deferCloseToEventThread(String id) {
        pendingCloseId = id;
        LOGGER.info("Incremental snapshot window close '{}' arrived outside the replication stream's thread; it runs at the next commit or heartbeat", id);
    }

    /** The window close deferred to the stream's thread, if any; clears it. */
    public String takePendingClose() {
        final String id = pendingCloseId;
        if (id != null) {
            pendingCloseId = null;
        }
        return id;
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
            backoffActive = false;
            retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
            return true;
        }
        readPending = true;
        backoffActive = true;
        retryNotBeforeNanos = nanoClock.getAsLong() + retryDelayNanos;
        retryDelayNanos = Math.min(retryDelayNanos * 2, MAX_RETRY_DELAY_NANOS);
        return false;
    }

    /** Whether a put-off chunk read is due now; clears the pending flag when it is. */
    public boolean retryDue() {
        if (!readPending || (backoffActive && nanoClock.getAsLong() - retryNotBeforeNanos < 0)) {
            return false;
        }
        readPending = false;
        return true;
    }

    /** Records the transaction of an emitted change, while a snapshot runs. */
    public void recordEmitted(OffsetContext offsetContext) {
        if (offsetContext == null || !guard.isTracking()) {
            return;
        }
        guard.recordEmitted(offsetContext.getSourceInfo().getInt64(SourceInfo.TXID_KEY));
    }

    public void pruneIfOversized() {
        try {
            guard.pruneIfOversized();
        }
        catch (SQLException | RuntimeException e) {
            LOGGER.warn("Pruning the commit-visibility set failed; it is retried with the next check", e);
        }
    }

    /** The snapshot ended or stopped: forget the tracked transactions and any put-off read. */
    public void snapshotEnded() {
        guard.stopTracking();
        readPending = false;
        backoffActive = false;
        retryDelayNanos = INITIAL_RETRY_DELAY_NANOS;
    }

    boolean isReadPending() {
        return readPending;
    }
}
