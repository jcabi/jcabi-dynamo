/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Credentials;
import com.jcabi.dynamo.Dosage;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import com.jcabi.dynamo.Valve;
import java.util.Collection;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Test case for {@link MkFrame}.
 * @since 0.10
 */
final class MkFrameTest {

    @Test
    void filtersWithWhereClause() throws Exception {
        final String name = "wh\u00e9re";
        final String key = "k\u00e9y";
        final String attr = "\u00e4ttr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes().with(key, "1").with(attr, "f\u00f6o")
        );
        table.put(
            new Attributes().with(key, "2").with(attr, "b\u00e4r")
        );
        MatcherAssert.assertThat(
            "does not filter items by where clause",
            table.frame().where(key, "1").size(),
            Matchers.equalTo(1)
        );
    }

    @Test
    void returnsTableReference() throws Exception {
        final String name = "t\u00e4ble";
        MatcherAssert.assertThat(
            "does not return table from frame",
            new MkRegion(
                new H2Data().with(name, new String[]{"cl\u00e9"})
            ).table(name).frame().table(),
            Matchers.notNullValue()
        );
    }

    @Test
    void countsSizeOfItems() throws Exception {
        final String name = "s\u00edze";
        final String key = "k\u00e9y";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key})
        );
        final Table table = region.table(name);
        table.put(new Attributes().with(key, "73921"));
        table.put(new Attributes().with(key, "48302"));
        MatcherAssert.assertThat(
            "does not count items correctly",
            table.frame().size(),
            Matchers.equalTo(2)
        );
    }

    @Test
    void ignoresValveInThrough() throws Exception {
        final String name = "d\u00fcrch";
        final String key = "k\u00e9y";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key})
        );
        final Table table = region.table(name);
        table.put(new Attributes().with(key, "91234"));
        final Frame frame = table.frame();
        MatcherAssert.assertThat(
            "does not return same frame from through",
            frame.through(
                new Valve() {
                    @Override
                    public Dosage fetch(
                        final Credentials credentials,
                        final String tbl,
                        final Map<String, Condition> conditions,
                        final Collection<String> keys) {
                        return new Dosage.Empty();
                    }

                    @Override
                    public int count(
                        final Credentials credentials,
                        final String tbl,
                        final Map<String, Condition> conditions) {
                        return 0;
                    }
                }
            ).size(),
            Matchers.equalTo(1)
        );
    }

    @Test
    void iteratesOverEmptyFrame() throws Exception {
        final String name = "\u00e9mpty";
        MatcherAssert.assertThat(
            "does not iterate over empty frame",
            new MkRegion(
                new H2Data().with(name, new String[]{"schl\u00fcssel"})
            ).table(name).frame().iterator().hasNext(),
            Matchers.is(false)
        );
    }
}
