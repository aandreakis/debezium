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
import java.util.Set;
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
 * <li><b>Tracking.</b> From the start of the task, the id of every transaction with an emitted change is tracked until
 * a snapshot shows it finished: its full id is below the snapshot's {@code xmin}, or below its {@code xmax} and not in
 * its list of running transactions. On a primary a finished transaction stays finished in every later snapshot. The
 * stream's 32-bit id is mapped to the full 64-bit id when it is recorded, against a snapshot at most 10 s old, and the
 * set is pruned at a commit or an idle heartbeat at most once a second.</li>
 * <li><b>Capture.</b> When tracking starts, at the start of the task and after a standby is promoted, transactions
 * emitted earlier are not known. A transaction keeps the lock on its own transaction id, and a lock on every table it
 * wrote, until after it has become visible: PostgreSQL removes a committing transaction from the list of running
 * transactions before it releases its locks. So every transaction that was emitted earlier and is still invisible shows
 * up in {@code pg_locks} with its transaction id and its tables. The guard takes a snapshot, reads {@code pg_locks}, and
 * tracks the transactions that hold a write lock on a captured table and that the snapshot does not show finished.</li>
 * <li><b>Check.</b> The guard prunes the tracked set with a new snapshot and, if transactions remain, polls new
 * snapshots (every 1 ms, doubling to every 50 ms) for at most {@code maxWait} without processing events, so the set can
 * only shrink. A tracked transaction pending longer than {@code maxWait} makes the check fail at once.</li>
 * </ul>
 * On a standby there is no gap: a transaction becomes visible when its commit record is replayed, before logical
 * decoding on the standby can read it. The guard then tracks nothing, and every check asks whether the server is still
 * a standby. After a promotion it captures and tracks as on a primary.
 * <p>
 * When a query of the guard fails, the guard forgets what it tracked and captures again before the next check.
 * <p>
 * Not thread-safe: used only by the thread that processes the replication stream.
 */
@NotThreadSafe
public class PostgresCommitVisibilityGuard {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCommitVisibilityGuard.class);

    static final long INITIAL_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    static final long MAX_POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
    static final long PRUNE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long REFERENCE_MAX_AGE_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_IDS_IN_LOG = 10;
    private static final int INITIAL_CAPACITY = 16;
    private static final long UNSIGNED_INT_MASK = 0xFFFF_FFFFL;

    /**
     * Database access of the guard. Each method ends the transaction it used before it returns, so every snapshot is
     * new and the connection is never left idle in a transaction.
     */
    public interface SnapshotSource {

        /**
         * Whether the server is in recovery and its current snapshot, in one round trip.
         */
        ServerState currentState() throws SQLException;

        /**
         * The transaction ids (the low 32 bits) of the running transactions that hold a granted lock stronger than
         * ROW SHARE on a captured table of the current database, read from {@code pg_locks}.
         */
        Set<Long> runningWritersOfCapturedTables() throws SQLException;
    }

    /**
     * Whether the server is a standby, and its current snapshot.
     */
    public static final class ServerState {
        private final boolean inRecovery;
        private final PgSnapshot snapshot;

        public ServerState(boolean inRecovery, PgSnapshot snapshot) {
            this.inRecovery = inRecovery;
            this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        }

        public boolean inRecovery() {
            return inRecovery;
        }

        public PgSnapshot snapshot() {
            return snapshot;
        }
    }

    @FunctionalInterface
    interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    enum Mode {
        /** Not started, or a query failed: the next check captures. */
        UNKNOWN,
        /** The server is a standby: nothing is tracked. */
        STANDBY,
        /** The server is a primary: emitted transactions are tracked. */
        PRIMARY
    }

    private final SnapshotSource source;
    private final long maxWaitNanos;
    private final LongSupplier nanoClock;
    private final Sleeper sleeper;

    /** Tracked full 64-bit transaction ids. */
    private long[] ids = new long[INITIAL_CAPACITY];
    private long[] pendingSinceNanos = new long[INITIAL_CAPACITY];
    private int size;

    private Mode mode = Mode.UNKNOWN;
    /** The raw id of the transaction recorded last; reset at every commit, so it is compared within a transaction. */
    private long lastRecordedRaw = -1L;
    private PgSnapshot reference;
    private long referenceAtNanos;
    private long lastPruneNanos;
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

    /**
     * Starts tracking at the start of the task: on a primary with a capture, on a standby without. A failure is logged
     * and the next check tries again.
     */
    public void start() {
        try {
            begin(source.currentState());
        }
        catch (SQLException | RuntimeException e) {
            lose("Starting the commit-visibility check failed; it starts again before the next chunk read", e);
        }
    }

    /**
     * Records the transaction of an emitted change.
     *
     * @param transactionId the transaction id from the change's source info: the stream's unsigned 32-bit id, or a
     *            full 64-bit id
     */
    public void recordEmitted(Long transactionId) {
        if (mode != Mode.PRIMARY || transactionId == null || transactionId == lastRecordedRaw) {
            return;
        }
        lastRecordedRaw = transactionId;
        try {
            final long now = nanoClock.getAsLong();
            if (transactionId > UNSIGNED_INT_MASK) {
                add(transactionId, now);
                return;
            }
            if (reference == null || now - referenceAtNanos >= REFERENCE_MAX_AGE_NANOS) {
                final ServerState state = source.currentState();
                if (state.inRecovery()) {
                    throw new IllegalStateException("The server became a standby");
                }
                prune(state.snapshot(), now);
            }
            add(toFullTransactionId(reference.getXMax(), transactionId), now);
        }
        catch (SQLException | RuntimeException e) {
            lose("Tracking an emitted transaction failed; the commit-visibility check captures again before the next chunk read", e);
        }
    }

    /**
     * A transaction's commit was processed: the next recorded id belongs to another transaction.
     */
    public void transactionCommitted() {
        lastRecordedRaw = -1L;
    }

    /**
     * At a commit or an idle heartbeat: prunes the tracked set with a new snapshot at most once a second.
     */
    public void maintain() {
        if (mode != Mode.PRIMARY || size == 0) {
            return;
        }
        final long now = nanoClock.getAsLong();
        if (now - lastPruneNanos < PRUNE_INTERVAL_NANOS) {
            return;
        }
        try {
            final ServerState state = source.currentState();
            if (state.inRecovery()) {
                throw new IllegalStateException("The server became a standby");
            }
            prune(state.snapshot(), now);
        }
        catch (SQLException | RuntimeException e) {
            lose("Pruning the commit-visibility set failed; the check captures again before the next chunk read", e);
        }
    }

    /**
     * Returns whether every tracked transaction is visible to new snapshots, polling for at most {@code maxWait}.
     * Processes no events. Captures first if tracking has not started or was lost.
     *
     * @throws SQLException if a query fails; tracking is then lost and the next check captures again
     */
    public boolean awaitVisible() throws SQLException, InterruptedException {
        try {
            return check();
        }
        catch (SQLException | RuntimeException e) {
            lose(null, null);
            throw e;
        }
    }

    private boolean check() throws SQLException, InterruptedException {
        ServerState state = source.currentState();
        if (mode == Mode.UNKNOWN || (mode == Mode.STANDBY && !state.inRecovery())) {
            if (mode == Mode.STANDBY) {
                LOGGER.info("The database is no longer a standby; chunk reads now wait for the visibility of emitted transactions");
            }
            begin(state);
            state = source.currentState();
        }
        if (mode == Mode.STANDBY) {
            passed();
            return true;
        }
        if (state.inRecovery()) {
            throw new IllegalStateException("The server became a standby");
        }
        final long pollStartedAt = nanoClock.getAsLong();
        prune(state.snapshot(), pollStartedAt);
        if (size == 0) {
            passed();
            return true;
        }
        if (oldestPendingAgeNanos(pollStartedAt) > maxWaitNanos) {
            failed(pollStartedAt, true);
            return false;
        }
        long intervalNanos = INITIAL_POLL_INTERVAL_NANOS;
        while (true) {
            final long remainingNanos = maxWaitNanos - (nanoClock.getAsLong() - pollStartedAt);
            if (remainingNanos <= 0) {
                failed(nanoClock.getAsLong(), false);
                return false;
            }
            sleeper.sleepNanos(Math.min(intervalNanos, remainingNanos));
            intervalNanos = Math.min(intervalNanos * 2, MAX_POLL_INTERVAL_NANOS);
            final ServerState polled = source.currentState();
            if (polled.inRecovery()) {
                throw new IllegalStateException("The server became a standby");
            }
            prune(polled.snapshot(), nanoClock.getAsLong());
            if (size == 0) {
                passed();
                return true;
            }
        }
    }

    public int unconfirmedCount() {
        return size;
    }

    int consecutiveFailedChecks() {
        return consecutiveFailedChecks;
    }

    Mode mode() {
        return mode;
    }

    /** The tracked full ids, for tests. */
    long[] trackedIds() {
        return Arrays.copyOf(ids, size);
    }

    /**
     * Starts tracking from the given state: on a standby nothing is tracked; on a primary the guard captures.
     */
    private void begin(ServerState state) throws SQLException {
        size = 0;
        lastRecordedRaw = -1L;
        reference = null;
        if (state.inRecovery()) {
            if (mode != Mode.STANDBY) {
                LOGGER.info("The database is a standby: transactions become visible before they can be decoded, so chunk reads do not wait for visibility");
            }
            mode = Mode.STANDBY;
            return;
        }
        final PgSnapshot snapshot = state.snapshot();
        final long now = nanoClock.getAsLong();
        prune(snapshot, now);
        final Set<Long> writers = source.runningWritersOfCapturedTables();
        for (Long writer : writers) {
            final long id = toFullTransactionId(snapshot.getXMax(), writer & UNSIGNED_INT_MASK);
            if (!isFinished(snapshot, id)) {
                add(id, now);
            }
        }
        mode = Mode.PRIMARY;
        if (!writers.isEmpty()) {
            LOGGER.info(
                    "Commit-visibility check started: {} running transaction(s) hold write locks on captured tables, {} of them not yet visible; chunk reads wait until those end. Snapshot {}:{}",
                    writers.size(), size, snapshot.getXMin(), snapshot.getXMax());
        }
    }

    private void lose(String message, Exception cause) {
        if (message != null) {
            LOGGER.warn(message, cause);
        }
        mode = Mode.UNKNOWN;
        size = 0;
        lastRecordedRaw = -1L;
        reference = null;
        if (ids.length > INITIAL_CAPACITY) {
            resize(INITIAL_CAPACITY);
        }
    }

    private void passed() {
        consecutiveFailedChecks = 0;
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
        final int shown = Math.min(size, MAX_IDS_IN_LOG);
        final boolean[] taken = new boolean[size];
        final StringBuilder text = new StringBuilder("[");
        for (int n = 0; n < shown; n++) {
            int oldest = -1;
            for (int i = 0; i < size; i++) {
                if (!taken[i] && (oldest < 0 || pendingSinceNanos[i] < pendingSinceNanos[oldest])) {
                    oldest = i;
                }
            }
            taken[oldest] = true;
            if (n > 0) {
                text.append(", ");
            }
            text.append(ids[oldest]).append(" (").append(TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - pendingSinceNanos[oldest]))).append(" ms)");
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
     * same low 32 bits in {@code [xmax - 2^31, xmax + 2^31)}. The id of a transaction that was just decoded is far
     * closer than 2^31 to the {@code xmax} of a snapshot at most 10 s old.
     */
    static long toFullTransactionId(long xmax, long transactionId32) {
        return xmax + (int) (transactionId32 - (xmax & UNSIGNED_INT_MASK));
    }

    static boolean isFinished(PgSnapshot snapshot, long fullTransactionId) {
        return fullTransactionId < snapshot.getXMin()
                || (fullTransactionId < snapshot.getXMax() && !snapshot.getXip().contains(fullTransactionId));
    }

    private void prune(PgSnapshot snapshot, long now) {
        int kept = 0;
        for (int i = 0; i < size; i++) {
            if (isFinished(snapshot, ids[i])) {
                continue;
            }
            ids[kept] = ids[i];
            pendingSinceNanos[kept] = pendingSinceNanos[i];
            kept++;
        }
        size = kept;
        reference = snapshot;
        referenceAtNanos = now;
        lastPruneNanos = now;
        if (ids.length > 4 * INITIAL_CAPACITY && size < ids.length / 4) {
            resize(Math.max(INITIAL_CAPACITY, ids.length / 2));
        }
    }

    private void add(long id, long pendingSince) {
        if (size == ids.length) {
            resize(ids.length * 2);
        }
        ids[size] = id;
        pendingSinceNanos[size] = pendingSince;
        size++;
    }

    private void resize(int capacity) {
        ids = Arrays.copyOf(ids, capacity);
        pendingSinceNanos = Arrays.copyOf(pendingSinceNanos, capacity);
    }
}
