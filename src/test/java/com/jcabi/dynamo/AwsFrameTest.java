/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.io.IOException;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Test case for {@link AwsFrame}.
 * @since 0.1
 */
final class AwsFrameTest {

    @Test
    void returnsAssociatedTable() {
        final AwsTable table = Mockito.mock(AwsTable.class);
        MatcherAssert.assertThat(
            "did not return associated table",
            new AwsFrame(
                Mockito.mock(Credentials.class), table, "t\u00e4bl"
            ).table(),
            Matchers.equalTo(table)
        );
    }

    @Test
    void createsFrameWithStringCondition() {
        MatcherAssert.assertThat(
            "did not create frame with string condition",
            new AwsFrame(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsTable.class),
                "str\u00efng-tbl"
            ).where("n\u00e4me", "v\u00e4lue"),
            Matchers.instanceOf(Frame.class)
        );
    }

    @Test
    void createsFrameWithCondition() {
        MatcherAssert.assertThat(
            "did not create frame with condition",
            new AwsFrame(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsTable.class),
                "c\u00f6nd-tbl"
            ).where("n\u00e4me", Condition.builder().build()),
            Matchers.instanceOf(Frame.class)
        );
    }

    @Test
    void createsFrameWithMapOfConditions() {
        MatcherAssert.assertThat(
            "did not create frame with map of conditions",
            new AwsFrame(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsTable.class),
                "m\u00e4p-tbl"
            ).where(
                Collections.singletonMap(
                    "\u00e4ttr", Condition.builder().build()
                )
            ),
            Matchers.instanceOf(Frame.class)
        );
    }

    @Test
    void changesValveViaThrough() {
        MatcherAssert.assertThat(
            "did not create frame with different valve",
            new AwsFrame(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsTable.class),
                "fl\u00f6w-tbl"
            ).through(Mockito.mock(Valve.class)),
            Matchers.instanceOf(Frame.class)
        );
    }

    @Test
    void delegatesSizeToValve() throws IOException {
        final Valve valve = Mockito.mock(Valve.class);
        final int expected = 42;
        Mockito.doReturn(expected).when(valve)
            .count(Mockito.any(), Mockito.anyString(), Mockito.any());
        MatcherAssert.assertThat(
            "did not delegate size to valve",
            new AwsFrame(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsTable.class),
                "s\u00efze-tbl", new Conditions(), valve
            ).size(),
            Matchers.equalTo(expected)
        );
    }

    @Test
    void wrapsExceptionOnSize() {
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> {
                final Valve valve = Mockito.mock(Valve.class);
                Mockito.doThrow(new IOException("b\u00f6om"))
                    .when(valve).count(
                        Mockito.any(),
                        Mockito.anyString(),
                        Mockito.any()
                    );
                new AwsFrame(
                    Mockito.mock(Credentials.class),
                    Mockito.mock(AwsTable.class),
                    "wr\u00e4p-tbl", new Conditions(), valve
                ).size();
            }
        );
    }

}
