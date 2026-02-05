/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import com.jcabi.dynamo.Credentials;
import com.jcabi.dynamo.Dosage;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Valve;
import com.jcabi.dynamo.mock.H2Data;
import com.jcabi.dynamo.mock.MkRegion;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Test case for {@link ReFrame}.
 * @since 0.9
 */
final class ReFrameTest {

    @Test
    void delegatesWhereWithNameAndValue() throws Exception {
        MatcherAssert.assertThat(
            "does not wrap where result in ReFrame",
            new ReFrame(ReFrameTest.origin()).where("k\u00e9y", "1"),
            Matchers.instanceOf(ReFrame.class)
        );
    }

    @Test
    void delegatesWhereWithCondition() throws Exception {
        MatcherAssert.assertThat(
            "does not wrap where-with-condition result in ReFrame",
            new ReFrame(ReFrameTest.origin()).where(
                "k\u00e9y", Conditions.equalTo("1")
            ),
            Matchers.instanceOf(ReFrame.class)
        );
    }

    @Test
    void delegatesTableAsReTable() throws Exception {
        MatcherAssert.assertThat(
            "does not wrap table in ReTable",
            new ReFrame(ReFrameTest.origin()).table(),
            Matchers.instanceOf(ReTable.class)
        );
    }

    @Test
    void delegatesSizeToOrigin() throws Exception {
        MatcherAssert.assertThat(
            "does not delegate size to origin frame",
            new ReFrame(ReFrameTest.origin()).size(),
            Matchers.equalTo(1)
        );
    }

    @Test
    void delegatesIteratorAsReIterator() throws Exception {
        MatcherAssert.assertThat(
            "does not wrap iterator in ReIterator",
            new ReFrame(ReFrameTest.origin()).iterator(),
            Matchers.instanceOf(ReIterator.class)
        );
    }

    @Test
    void delegatesThroughReturningReFrame() throws Exception {
        MatcherAssert.assertThat(
            "does not return ReFrame from through",
            new ReFrame(ReFrameTest.origin()).through(
                new Valve() {
                    @Override
                    public Dosage fetch(
                        final Credentials credentials,
                        final String table,
                        final Map<String, Condition> conditions,
                        final Collection<String> keys) {
                        return new Dosage.Empty();
                    }

                    @Override
                    public int count(
                        final Credentials credentials,
                        final String table,
                        final Map<String, Condition> conditions) {
                        return 0;
                    }
                }
            ),
            Matchers.instanceOf(ReFrame.class)
        );
    }

    @Test
    void delegatesIsEmptyToOrigin() throws Exception {
        MatcherAssert.assertThat(
            "does not delegate isEmpty to origin",
            new ReFrame(ReFrameTest.origin()).isEmpty(),
            Matchers.is(false)
        );
    }

    /**
     * Creates a frame backed by H2Data with one item.
     * @return Frame with one item
     * @throws IOException If fails
     */
    private static Frame origin() throws IOException {
        final String table = "t\u00e9sts";
        final String key = "k\u00e9y";
        final String attr = "\u00e4ttr";
        final H2Data data = new H2Data().with(
            table, new String[]{key}, attr
        );
        data.put(
            table,
            new Attributes().with(key, "1").with(attr, "v\u00e4l")
        );
        return new MkRegion(data).table(table).frame();
    }
}
