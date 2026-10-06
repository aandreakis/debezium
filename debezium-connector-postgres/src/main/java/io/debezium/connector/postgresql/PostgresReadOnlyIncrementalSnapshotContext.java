/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;

/**
 * A class describing PostgreSQL read only incremental snapshot context state.
 * <p>
 * A chunk's window opens when the chunk is read and is released once the replication stream has been read up to the
 * <em>release position</em>, a WAL position read right after the chunk query that is at or after the end of the commit
 * record of every transaction the query saw. By then every one of those transactions has been processed, and an event
 * processed in between has removed the buffered row with the same key.
 * <p>
 * The window is released at the first processed commit whose position (the end of its commit record) is at or after
 * the release position, or on an idle stream once the server's sent position reaches it. When the release position is
 * a standby's replay position, the commit record of a transaction the query saw may still be in replay and end after
 * it; the stream must then pass the release position, not just reach it, unless the source shows that no record was
 * left in replay ({@link #waitsAtReleasePosition(Lsn)}).
 *
 * @author Mario Fiore Vitale
 */
public class PostgresReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotContext.class);

    private Lsn releasePosition;
    private boolean mustPass;

    public PostgresReadOnlyIncrementalSnapshotContext() {
        this(false);
    }

    public PostgresReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    public static <U> PostgresReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, false);
    }

    public static <U> PostgresReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        PostgresReadOnlyIncrementalSnapshotContext<U> context = new PostgresReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    /**
     * A chunk was read; its window is released once the stream has been read up to {@code position}.
     *
     * @param mustPass whether the stream must pass {@code position} rather than reach it
     */
    public void chunkRead(Lsn position, boolean mustPass) {
        LOGGER.trace("Chunk read, window released at stream position {}{}", position, mustPass ? " (must be passed)" : "");
        this.releasePosition = position;
        this.mustPass = mustPass;
        windowOpened = true;
    }

    /**
     * Whether the window can be released after processing a commit whose commit record ends at {@code commitEnd}.
     */
    public boolean releaseDueAtCommit(Lsn commitEnd) {
        return reached(commitEnd);
    }

    /**
     * Whether the window can be released on an idle stream whose server has sent up to {@code serverPosition}.
     */
    public boolean releaseDueWhenIdle(Lsn serverPosition) {
        return reached(serverPosition);
    }

    /**
     * Whether the stream has reached, but must pass, the release position: on an idle standby the source can then
     * release the window once it sees that no record was left in replay.
     */
    public boolean waitsAtReleasePosition(Lsn serverPosition) {
        return windowOpened && mustPass && releasePosition != null && serverPosition != null && serverPosition.isValid()
                && serverPosition.compareTo(releasePosition) >= 0;
    }

    private boolean reached(Lsn streamPosition) {
        if (!windowOpened || releasePosition == null || streamPosition == null || !streamPosition.isValid()) {
            return false;
        }
        final int comparison = streamPosition.compareTo(releasePosition);
        return mustPass ? comparison > 0 : comparison >= 0;
    }

    public void releaseWindow() {
        LOGGER.trace("Window released at release position {}", releasePosition);
        windowOpened = false;
        releasePosition = null;
        mustPass = false;
    }

    public boolean isWindowOpen() {
        return windowOpened;
    }

    public Lsn getReleasePosition() {
        return releasePosition;
    }
}
