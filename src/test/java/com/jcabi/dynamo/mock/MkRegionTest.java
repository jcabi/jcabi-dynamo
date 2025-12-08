/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link MkRegion}.
 * @since 0.10
 * @checkstyle MultipleStringLiteralsCheck (500 lines)
 */
final class MkRegionTest {

    @Test
    void storesAndReadsAttributes() throws Exception {
        final String name = "users";
        final String key = "id";
        final String attr = "description";
        final String nattr = "thenumber";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[] {key}, attr, nattr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "32443")
                .with(attr, "first value to \n\t€ save")
                .with(nattr, "150")
        );
        final Item item = table.frame().iterator().next();
        MatcherAssert.assertThat("should be true", item.has(attr), Matchers.is(true));
        MatcherAssert.assertThat(
            "should contains '\n\t\u20ac save'",
            item.get(attr).getS(),
            Matchers.containsString("\n\t\u20ac save")
        );
        item.put(
            attr,
            new AttributeValueUpdate().withValue(
                new AttributeValue("this is another value")
            )
        );
        MatcherAssert.assertThat(
            "should contains 'another value'",
            item.get(attr).getS(),
            Matchers.containsString("another value")
        );
        MatcherAssert.assertThat(
            "should ends with '50'",
            item.get(nattr).getN(),
            Matchers.endsWith("50")
        );
    }

    @Test
    void storesAndReadsSingleAttribute() throws Exception {
        final String table = "ideas";
        final String key = "number";
        final String attr = "total";
        final Region region = new MkRegion(
            new H2Data().with(table, new String[] {key}, attr)
        );
        final Table tbl = region.table(table);
        tbl.put(
            new Attributes()
                .with(key, "324439")
                .with(attr, "0")
        );
        final Item item = tbl.frame().iterator().next();
        item.put(
            attr,
            new AttributeValueUpdate().withValue(
                new AttributeValue().withN("2")
            ).withAction(AttributeAction.PUT)
        );
        MatcherAssert.assertThat("should equal 2", item.get(attr).getN(), Matchers.equalTo("2"));
    }

}
