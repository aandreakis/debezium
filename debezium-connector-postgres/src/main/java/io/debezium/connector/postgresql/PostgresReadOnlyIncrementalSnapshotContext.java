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
 * <em>release position</em>, a WAL position read right after the chunk query. Every transaction the chunk query saw
 * committed before that position, so by then every one of them has been processed, and an event processed in between
 * has removed the buffered row with the same key.
 *
 * @author Mario Fiore Vitale
 */
public class PostgresReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotContext.class);

    private Lsn releasePosition;

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
     */
    public void chunkRead(Lsn position) {
        LOGGER.trace("Chunk read, window released at stream position {}", position);
        releasePosition = position;
        windowOpened = true;
    }

    /**
     * Whether the window of the chunk read last can be released at the given stream position.
     */
    public boolean releaseDue(Lsn streamPosition) {
        return windowOpened && releasePosition != null && streamPosition != null && streamPosition.isValid()
                && streamPosition.compareTo(releasePosition) >= 0;
    }

    public void releaseWindow() {
        LOGGER.trace("Window released at release position {}", releasePosition);
        windowOpened = false;
        releasePosition = null;
    }

    public boolean isWindowOpen() {
        return windowOpened;
    }

    public Lsn getReleasePosition() {
        return releasePosition;
    }
}
