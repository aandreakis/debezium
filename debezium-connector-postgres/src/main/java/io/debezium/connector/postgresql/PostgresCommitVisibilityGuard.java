/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;

/**
 * Commit-visibility check for incremental snapshot chunk reads on PostgreSQL.
 * <p>
 * On a PostgreSQL primary a transaction reaches the logical replication stream as soon as its commit record is
 * flushed, but new snapshots see it only after it leaves the list of running transactions. The gap is usually short.
 * It grows under CPU pressure, and it has no bound while the commit waits for a synchronous standby. A chunk query that
 * runs in that gap does not see a change whose event the connector has already emitted, and the chunk's READ event,
 * emitted later, carries the older row.
 * <p>
 * The guard lets a chunk read run only when a snapshot taken after the read decision shows every tracked transaction
 * finished:
 * <ul>
 * <li><b>Tracking.</b> While an incremental snapshot runs, the id of every transaction with an emitted change is
 * tracked until a snapshot shows the transaction finished: its full id is below the snapshot's {@code xmin}, or below
 * its {@code xmax} and not in its list of running transactions. On a primary a finished transaction stays finished in
 * every later snapshot. The stream's 32-bit id is mapped to the full 64-bit id at the first snapshot after it was
 * recorded.</li>
 * <li><b>Capture.</b> When tracking starts, at the first check of a snapshot (also after a connector restart), the
 * guard assigns a fresh transaction id and ends that transaction, then takes a snapshot and tracks every transaction
 * the snapshot lists as running. A transaction whose changes were emitted before tracking started, and that is still
 * invisible, got its id before the fresh one, so ending the fresh one makes the next snapshot list it as running.</li>
 * <li><b>Check.</b> The guard prunes the tracked set with a new snapshot and, if transactions remain, polls new
 * snapshots (every 1 ms, doubling to every 50 ms) without processing events, so the set can only shrink. A check polls
 * for at most {@code min(maxWait, 1 s * 2^k)}, where k counts the consecutive failed checks; a tracked transaction
 * pending longer than {@code maxWait} makes the check fail at once.</li>
 * </ul>
 * On a standby there is no gap: a transaction becomes visible when its commit record is replayed, before it can be
 * decoded. The guard then disables itself.
 * <p>
 * Not thread-safe: used only by the thread that processes the replication stream.
 */
@NotThreadSafe
public class PostgresCommitVisibilityGuard {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCommitVisibilityGuard.class);

    static final long INITIAL_BUDGET_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long INITIAL_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    static final long MAX_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    static final int PRUNE_SIZE_THRESHOLD = 100_000;
    private static final long LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_IDS_IN_LOG = 10;
    private static final int INITIAL_CAPACITY = 16;
    private static final long UNSIGNED_INT_MASK = 0xFFFF_FFFFL;

    /**
     * Database access of the guard. Each method runs its statements in a transaction of its own, so that every
     * snapshot is new whatever the session's isolation level.
     */
    public interface SnapshotSource {

        boolean isInRecovery() throws SQLException;

        /**
         * Assigns a transaction id in a new transaction and ends that transaction.
         */
        void assignAndEndTransactionId() throws SQLException;

        /**
         * The current snapshot as text, {@code xmin:xmax:xip,...} with full 64-bit ids.
         */
        String currentSnapshot() throws SQLException;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    private final SnapshotSource source;
    private final long maxWaitNanos;
    private final LongSupplier nanoClock;
    private final Sleeper sleeper;

    /** Tracked ids: the full 64-bit id when {@code mapped[i]}, else the stream's unsigned 32-bit id. */
    private long[] ids = new long[INITIAL_CAPACITY];
    private boolean[] mapped = new boolean[INITIAL_CAPACITY];
    private long[] pendingSinceNanos = new long[INITIAL_CAPACITY];
    private int size;

    private boolean tracking;
    private boolean disabled;
    private long lastRecorded = -1L;
    private int consecutiveFailedChecks;
    private long failureLoggedAtNanos;
    private boolean failureLogged;

    public PostgresCommitVisibilityGuard(SnapshotSource source, Duration maxWait) {
        this(source, maxWait, System::nanoTime, nanos -> TimeUnit.NANOSECONDS.sleep(nanos));
    }

    PostgresCommitVisibilityGuard(SnapshotSource source, Duration maxWait, LongSupplier nanoClock, Sleeper sleeper) {
        this.source = Objects.requireNonNull(source, "source");
        if (maxWait.isNegative() || maxWait.isZero()) {
            throw new IllegalArgumentException("maxWait must be positive");
        }
        this.maxWaitNanos = maxWait.toNanos();
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    public boolean isTracking() {
        return tracking;
    }

    /**
     * Records the transaction of an emitted change while tracking; ignored otherwise.
     *
     * @param transactionId the transaction id from the change's source info: the stream's unsigned 32-bit id, or a
     *            full 64-bit id
     */
    public void recordEmitted(Long transactionId) {
        if (!tracking || disabled || transactionId == null || transactionId == lastRecorded) {
            return;
        }
        lastRecorded = transactionId;
        add(transactionId, transactionId > UNSIGNED_INT_MASK, nanoClock.getAsLong());
    }

    /**
     * Ends tracking and forgets every tracked transaction; the next check captures again.
     */
    public void stopTracking() {
        tracking = false;
        size = 0;
        lastRecorded = -1L;
        consecutiveFailedChecks = 0;
        if (ids.length > INITIAL_CAPACITY) {
            resize(INITIAL_CAPACITY);
        }
    }

    /**
     * Starts tracking with a capture if it has not started yet, then returns whether every tracked transaction is
     * visible to new snapshots, polling within the budget. Processes no events.
     */
    public boolean awaitVisible() throws SQLException, InterruptedException {
        if (!tracking) {
            capture();
        }
        if (disabled || size == 0) {
            consecutiveFailedChecks = 0;
            return true;
        }
        prune(PgSnapshot.valueOf(source.currentSnapshot()));
        if (size == 0) {
            consecutiveFailedChecks = 0;
            return true;
        }
        final long pollStartedAt = nanoClock.getAsLong();
        if (oldestPendingAgeNanos(pollStartedAt) > maxWaitNanos) {
            failed(pollStartedAt, true);
            return false;
        }
        final long budgetNanos = Math.min(maxWaitNanos, INITIAL_BUDGET_NANOS << Math.min(consecutiveFailedChecks, 30));
        long intervalNanos = INITIAL_POLL_INTERVAL_NANOS;
        while (true) {
            final long remainingNanos = budgetNanos - (nanoClock.getAsLong() - pollStartedAt);
            if (remainingNanos <= 0) {
                failed(nanoClock.getAsLong(), false);
                return false;
            }
            sleeper.sleepNanos(Math.min(intervalNanos, remainingNanos));
            intervalNanos = Math.min(intervalNanos * 2, MAX_POLL_INTERVAL_NANOS);
            prune(PgSnapshot.valueOf(source.currentSnapshot()));
            if (size == 0) {
                consecutiveFailedChecks = 0;
                return true;
            }
        }
    }

    /**
     * Prunes the tracked set with a new snapshot once it holds more than 100,000 transactions.
     */
    public void pruneIfOversized() throws SQLException {
        if (size > PRUNE_SIZE_THRESHOLD) {
            prune(PgSnapshot.valueOf(source.currentSnapshot()));
        }
    }

    public int unconfirmedCount() {
        return size;
    }

    int consecutiveFailedChecks() {
        return consecutiveFailedChecks;
    }

    boolean isDisabled() {
        return disabled;
    }

    /** The tracked ids, 64-bit once mapped, for tests. */
    long[] trackedIds() {
        return Arrays.copyOf(ids, size);
    }

    private void capture() throws SQLException {
        if (source.isInRecovery()) {
            if (!disabled) {
                LOGGER.info("The database is a standby: transactions become visible before they can be decoded, so chunk reads do not wait for visibility");
            }
            disabled = true;
            tracking = true;
            return;
        }
        disabled = false;
        source.assignAndEndTransactionId();
        final PgSnapshot snapshot = PgSnapshot.valueOf(source.currentSnapshot());
        prune(snapshot);
        final long now = nanoClock.getAsLong();
        for (Long running : snapshot.getXip()) {
            add(running, true, now);
        }
        tracking = true;
        if (!snapshot.getXip().isEmpty()) {
            LOGGER.info("Incremental snapshot: {} transaction(s) were running when tracking started; the next chunk read waits until they end. Snapshot {}:{}:{}",
                    snapshot.getXip().size(), snapshot.getXMin(), snapshot.getXMax(), snapshot.getXip());
        }
    }

    private void failed(long now, boolean stuck) {
        if (consecutiveFailedChecks < Integer.MAX_VALUE) {
            consecutiveFailedChecks++;
        }
        if (!failureLogged || now - failureLoggedAtNanos >= LOG_INTERVAL_NANOS) {
            failureLogged = true;
            failureLoggedAtNanos = now;
            LOGGER.warn("Incremental snapshot chunk read deferred: {} transaction(s) whose changes were already emitted are not yet visible to new snapshots{}; "
                    + "oldest pending: {} (consecutive failed checks: {})",
                    size,
                    stuck ? ", one of them for longer than " + TimeUnit.NANOSECONDS.toMillis(maxWaitNanos) + " ms" : "",
                    oldestPending(now), consecutiveFailedChecks);
        }
    }

    private String oldestPending(long now) {
        final Integer[] order = new Integer[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (left, right) -> Long.compare(pendingSinceNanos[left], pendingSinceNanos[right]));
        final StringBuilder text = new StringBuilder("[");
        for (int i = 0; i < order.length && i < MAX_IDS_IN_LOG; i++) {
            if (i > 0) {
                text.append(", ");
            }
            text.append(ids[order[i]]).append(" (").append(TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - pendingSinceNanos[order[i]]))).append(" ms)");
        }
        return text.append(']').toString();
    }

    private long oldestPendingAgeNanos(long now) {
        long oldest = 0L;
        for (int i = 0; i < size; i++) {
            oldest = Math.max(oldest, now - pendingSinceNanos[i]);
        }
        return oldest;
    }

    /**
     * Maps a stream id (unsigned 32 bits) to the full id nearest to the snapshot's {@code xmax}: the unique id with the
     * same low 32 bits in {@code [xmax - 2^31, xmax + 2^31)}. A transaction already decoded is far closer to
     * {@code xmax} than 2^31, which wraparound protection guarantees.
     */
    static long toFullTransactionId(long xmax, long transactionId32) {
        return xmax + (int) (transactionId32 - (xmax & UNSIGNED_INT_MASK));
    }

    static boolean isFinished(PgSnapshot snapshot, long fullTransactionId) {
        return fullTransactionId < snapshot.getXMin()
                || (fullTransactionId < snapshot.getXMax() && !snapshot.getXip().contains(fullTransactionId));
    }

    private void prune(PgSnapshot snapshot) {
        final long xmax = snapshot.getXMax();
        int kept = 0;
        for (int i = 0; i < size; i++) {
            final long id = mapped[i] ? ids[i] : toFullTransactionId(xmax, ids[i]);
            if (isFinished(snapshot, id)) {
                continue;
            }
            ids[kept] = id;
            mapped[kept] = true;
            pendingSinceNanos[kept] = pendingSinceNanos[i];
            kept++;
        }
        size = kept;
        if (ids.length > 4 * INITIAL_CAPACITY && size < ids.length / 4) {
            resize(Math.max(INITIAL_CAPACITY, ids.length / 2));
        }
    }

    private void add(long id, boolean isMapped, long pendingSince) {
        if (size == ids.length) {
            resize(ids.length * 2);
        }
        ids[size] = id;
        mapped[size] = isMapped;
        pendingSinceNanos[size] = pendingSince;
        size++;
    }

    private void resize(int capacity) {
        ids = Arrays.copyOf(ids, capacity);
        mapped = Arrays.copyOf(mapped, capacity);
        pendingSinceNanos = Arrays.copyOf(pendingSinceNanos, capacity);
    }
}
