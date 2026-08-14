/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Credentials;
import com.jcabi.dynamo.Dosage;
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
        final String name = "whére";
        final String key = "kéy";
        final String attr = "ättr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes().with(key, "1").with(attr, "föo")
        );
        table.put(
            new Attributes().with(key, "2").with(attr, "bär")
        );
        MatcherAssert.assertThat(
            "does not filter items by where clause",
            table.frame().where(key, "1").size(),
            Matchers.equalTo(1)
        );
    }

    @Test
    void returnsTableReference() throws Exception {
        final String name = "täble";
        MatcherAssert.assertThat(
            "does not return table from frame",
            new MkRegion(
                new H2Data().with(name, new String[]{"clé"})
            ).table(name).frame().table(),
            Matchers.notNullValue()
        );
    }

    @Test
    void countsSizeOfItems() throws Exception {
        final String name = "síze";
        final String key = "kéy";
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
        final String name = "dürch";
        final String key = "kéy";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key})
        );
        final Table table = region.table(name);
        table.put(new Attributes().with(key, "91234"));
        MatcherAssert.assertThat(
            "does not return same frame from through",
            table.frame().through(
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
        final String name = "émpty";
        MatcherAssert.assertThat(
            "does not iterate over empty frame",
            new MkRegion(
                new H2Data().with(name, new String[]{"schlüssel"})
            ).table(name).frame().iterator().hasNext(),
            Matchers.is(false)
        );
    }
}
