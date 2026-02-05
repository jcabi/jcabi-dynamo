/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

/**
 * Test case for {@link MkItem}.
 * @since 0.10
 */
final class MkItemTest {

    @Test
    void returnsFrameReference() throws Exception {
        final String name = "fr\u00e4me";
        final String key = "k\u00e9y";
        final String attr = "\u00e4ttr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "28173")
                .with(attr, "v\u00e4lue")
        );
        MatcherAssert.assertThat(
            "does not return frame from item",
            table.frame().iterator().next().frame(),
            Matchers.notNullValue()
        );
    }

    @Test
    void checksAbsentAttributeReturnsFalse() throws Exception {
        final String name = "h\u00e4s";
        final String key = "k\u00e9y";
        final String attr = "\u00e4ttr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "91823")
                .with(attr, "v\u00e4lue")
        );
        MatcherAssert.assertThat(
            "does not return false for absent attribute",
            table.frame().iterator().next().has("nonexistent"),
            Matchers.is(false)
        );
    }

    @Test
    void putsMultipleAttributes() throws Exception {
        final String name = "m\u00fclti";
        final String key = "k\u00e9y";
        final String first = "f\u00edrst";
        final String second = "s\u00e9cond";
        final Region region = new MkRegion(
            new H2Data().with(
                name, new String[]{key}, first, second
            )
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "71234")
                .with(first, "old\u00f6ne")
                .with(second, "old\u00f6two")
        );
        final Item item = table.frame().iterator().next();
        item.put(
            first,
            AttributeValueUpdate.builder().value(
                AttributeValue.builder().s("n\u00e9w").build()
            ).build()
        );
        MatcherAssert.assertThat(
            "does not update attribute correctly",
            item.get(first).s(),
            Matchers.equalTo("n\u00e9w")
        );
    }
}
