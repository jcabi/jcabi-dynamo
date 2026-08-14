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
        final String name = "främe";
        final String key = "kéy";
        final String attr = "ättr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "28173")
                .with(attr, "välue")
        );
        MatcherAssert.assertThat(
            "does not return frame from item",
            table.frame().iterator().next().frame(),
            Matchers.notNullValue()
        );
    }

    @Test
    void checksAbsentAttributeReturnsFalse() throws Exception {
        final String name = "häs";
        final String key = "kéy";
        final String attr = "ättr";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[]{key}, attr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "91823")
                .with(attr, "välue")
        );
        MatcherAssert.assertThat(
            "does not return false for absent attribute",
            table.frame().iterator().next().has("nonexistent"),
            Matchers.is(false)
        );
    }

    @Test
    void putsMultipleAttributes() throws Exception {
        final String name = "mülti";
        final String key = "kéy";
        final String first = "fírst";
        final String second = "sécond";
        final Region region = new MkRegion(
            new H2Data().with(
                name, new String[]{key}, first, second
            )
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "71234")
                .with(first, "oldöne")
                .with(second, "oldötwo")
        );
        final Item item = table.frame().iterator().next();
        item.put(
            first,
            AttributeValueUpdate.builder().value(
                AttributeValue.builder().s("néw").build()
            ).build()
        );
        MatcherAssert.assertThat(
            "does not update attribute correctly",
            item.get(first).s(),
            Matchers.equalTo("néw")
        );
    }
}
