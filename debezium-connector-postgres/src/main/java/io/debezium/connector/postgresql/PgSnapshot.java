/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import io.debezium.DebeziumException;

/**
 * This class contains the information returned by the pg_current_snapshot function.
 *
 * @author Mario Fiore Vitale
 */
public class PgSnapshot {

    private final Long xMin;
    private final Long xMax;
    private final Set<Long> xip;

    public PgSnapshot(Long xMin, Long xMax, Set<Long> xip) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.xip = xip;
    }

    public Long getXMin() {
        return xMin;
    }

    public Long getXMax() {
        return xMax;
    }

    public Set<Long> getXip() {
        return xip;
    }

    /**
     * Returns a PgSnapshot instance representing the specified snapshot string
     *
     * @param snapshotString is the string returned by the pg_current_snapshot function
     * @return a PgSnapshot representing the {@code snapshotString}
     */
    public static PgSnapshot valueOf(String snapshotString) {
        // Parsed by hand: a regular expression with nested repetition overflows the stack on long lists of running
        // transactions.
        if (snapshotString != null) {
            final int firstColon = snapshotString.indexOf(':');
            final int secondColon = firstColon < 0 ? -1 : snapshotString.indexOf(':', firstColon + 1);
            if (firstColon > 0 && secondColon > firstColon + 1) {
                try {
                    final long xmin = parseId(snapshotString, 0, firstColon);
                    final long xmax = parseId(snapshotString, firstColon + 1, secondColon);
                    final Set<Long> xip = new HashSet<>();
                    int start = secondColon + 1;
                    while (start < snapshotString.length()) {
                        int end = snapshotString.indexOf(',', start);
                        if (end < 0) {
                            end = snapshotString.length();
                        }
                        if (end > start) {
                            xip.add(parseId(snapshotString, start, end));
                        }
                        start = end + 1;
                    }
                    return new PgSnapshot(xmin, xmax, xip);
                }
                catch (NumberFormatException e) {
                    // reported below
                }
            }
        }

        throw new DebeziumException(String.format("Unable to parse PgCurrentSnapshot result %s.", snapshotString));
    }

    private static long parseId(String text, int start, int end) {
        for (int i = start; i < end; i++) {
            final char c = text.charAt(i);
            if (c < '0' || c > '9') {
                throw new NumberFormatException(text.substring(start, end));
            }
        }
        return Long.parseLong(text, start, end, 10);
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PgSnapshot that = (PgSnapshot) o;
        return Objects.equals(xMin, that.xMin) && Objects.equals(xMax, that.xMax) && Objects.equals(xip, that.xip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(xMin, xMax, xip);
    }

    @Override
    public String toString() {
        return "PgSnapshot{" +
                "xMin=" + xMin +
                ", xMax=" + xMax +
                ", xip=" + xip +
                '}';
    }
}
