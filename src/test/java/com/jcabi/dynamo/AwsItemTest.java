/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.immutable.Array;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link AwsItem}.
 * @since 0.21
 */
final class AwsItemTest {

    @Test
    void comparesToItself() {
        final Credentials creds = new Credentials.Simple("key", "secret");
        final AwsTable table = new AwsTable(
            creds, new Region.Simple(creds), "table-name"
        );
        final AwsFrame frame = new AwsFrame(creds, table, table.name());
        MatcherAssert.assertThat(
            "should equal to itself",
            new AwsItem(
                creds, frame, table.name(),
                new Attributes(), new Array<>()
            ),
            Matchers.equalTo(
                new AwsItem(
                    creds, frame, table.name(),
                    new Attributes(), new Array<>()
                )
            )
        );
    }

}
