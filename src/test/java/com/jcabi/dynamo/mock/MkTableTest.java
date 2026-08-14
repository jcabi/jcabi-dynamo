/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link MkTable}.
 * @since 0.10
 */
final class MkTableTest {

    @Test
    void returnsTableName() throws Exception {
        final String name = "näme";
        MatcherAssert.assertThat(
            "does not return correct table name",
            new MkRegion(
                new H2Data().with(name, new String[]{"clé"})
            ).table(name).name(),
            Matchers.equalTo(name)
        );
    }

    @Test
    void returnsRegion() throws Exception {
        final String name = "région";
        MatcherAssert.assertThat(
            "does not return region",
            new MkRegion(
                new H2Data().with(name, new String[]{"schlüssel"})
            ).table(name).region(),
            Matchers.instanceOf(MkRegion.class)
        );
    }

    @Test
    void returnsFrame() throws Exception {
        final String name = "främe";
        MatcherAssert.assertThat(
            "does not return frame",
            new MkRegion(
                new H2Data().with(name, new String[]{"kéy"})
            ).table(name).frame(),
            Matchers.notNullValue()
        );
    }

    @Test
    void putsItemAndReturnsIt() throws Exception {
        final String name = "übung";
        final String key = "kéy";
        final String attr = "ättr";
        MatcherAssert.assertThat(
            "does not return item after put",
            new MkRegion(
                new H2Data().with(name, new String[]{key}, attr)
            ).table(name).put(
                new Attributes()
                    .with(key, "47381")
                    .with(attr, "välue")
            ),
            Matchers.notNullValue()
        );
    }

    @Test
    void deletesItem() throws Exception {
        final String name = "dél";
        final String key = "kéy";
        final String attr = "ättr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "89213")
                .with(attr, "to-délete")
        );
        table.delete(new Attributes().with(key, "89213"));
        MatcherAssert.assertThat(
            "does not delete item from table",
            table.frame(),
            Matchers.emptyIterable()
        );
    }
}
