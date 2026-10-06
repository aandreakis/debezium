/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

public class PgSnapshotTest {

    @Test
    void parseCorrectPgSnapshotWithInProgressTransactions() {

        PgSnapshot snapshot = PgSnapshot.valueOf("795:799:795,797");

        Assertions.assertThat(snapshot.getXMin()).isEqualTo(795L);
        Assertions.assertThat(snapshot.getXMax()).isEqualTo(799L);
        Assertions.assertThat(snapshot.getXip()).contains(795L, 797L);
    }

    @Test
    void parseCorrectPgSnapshotWithoutInProgressTransactions() {

        PgSnapshot snapshot = PgSnapshot.valueOf("795:799:");

        Assertions.assertThat(snapshot.getXMin()).isEqualTo(795L);
        Assertions.assertThat(snapshot.getXMax()).isEqualTo(799L);
        Assertions.assertThat(snapshot.getXip()).isEmpty();
    }

    @Test
    void parseAWrongPgSnapshotWillThrowException() {

        Assertions.assertThatThrownBy(() -> PgSnapshot.valueOf("795::"))
                .isInstanceOf(DebeziumException.class);
    }

    @Test
    void parseRejectsMalformedSnapshots() {
        for (String malformed : new String[]{ "795:799", ":799:", "795:799:a", "795:x:", "-1:799:", "795:799: 797", "" }) {
            Assertions.assertThatThrownBy(() -> PgSnapshot.valueOf(malformed))
                    .as(malformed)
                    .isInstanceOf(DebeziumException.class);
        }
        Assertions.assertThatThrownBy(() -> PgSnapshot.valueOf(null)).isInstanceOf(DebeziumException.class);
    }

    @Test
    void parseLongListOfRunningTransactionsWithoutStackOverflow() {
        final StringBuilder text = new StringBuilder("4294967396:4295067396:");
        for (long id = 4294967396L; id < 4295067396L; id++) {
            if (id > 4294967396L) {
                text.append(',');
            }
            text.append(id);
        }

        PgSnapshot snapshot = PgSnapshot.valueOf(text.toString());

        Assertions.assertThat(snapshot.getXMin()).isEqualTo(4294967396L);
        Assertions.assertThat(snapshot.getXMax()).isEqualTo(4295067396L);
        Assertions.assertThat(snapshot.getXip()).hasSize(100_000).contains(4294967396L, 4295067395L);
    }
}
