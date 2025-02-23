/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test case for {@link H2Data}.
 * @since 0.10
 */
final class H2DataTest {

    @Test
    void storesAndReadsAttributes() throws Exception {
        final String table = "users";
        final String key = "user";
        final int number = 43;
        final String attr = "user.name";
        final String value = "some\n\t\u20ac text";
        final MkData data = new H2Data().with(
            table, new String[] {key},
            attr
        );
        data.put(table, new Attributes().with(key, number).with(attr, value));
        MatcherAssert.assertThat(
            data.iterate(
                table, new Conditions().with(key, Conditions.equalTo(number))
            ).iterator().next(),
            Matchers.hasEntry(
                Matchers.equalTo(attr),
                Matchers.equalTo(new AttributeValue(value))
            )
        );
    }

    @Test
    @Disabled
    void storesToFile(@TempDir final Path temp) throws Exception {
        final File file = temp.resolve("foo.txt").toFile();
        final String table = "tbl";
        final String key = "user";
        final MkData data = new H2Data(file).with(
            table, new String[] {key}
        );
        data.put(table, new Attributes().with(key, "x2"));
        MatcherAssert.assertThat(file.exists(), Matchers.is(true));
        MatcherAssert.assertThat(file.length(), Matchers.greaterThan(0L));
    }

    @Test
    void createsManyTables() throws Exception {
        new H2Data()
            .with("firsttable", new String[] {"firstid"}, "test")
            .with("secondtable", new String[]{"secondid"});
    }

    @Test
    void createsTablesWithLongNames() throws Exception {
        new H2Data().with(
            Joiner.on("").join(Collections.nCopies(40, "X")),
            new String[]{"k1"}
        );
    }

    @Test
    void supportsTableNamesWithIllegalCharacters() throws Exception {
        new H2Data().with(".-.", new String[]{"pk"});
    }

    @Test
    @Disabled
    void supportsColumnNamesWithIllegalCharacters() throws Exception {
        final String key = "0-.col.-0";
        final String table = "test";
        new H2Data().with(
            table, new String[] {key}
        ).put(table, new Attributes().with(key, "value"));
    }

    @Test
    void deletesRecords() throws Exception {
        final String table = "customers";
        final String field = "name";
        final String man = "Kevin";
        final String woman = "Helen";
        final H2Data data = new H2Data()
            .with(table, new String[]{field});
        data.put(
            table,
            new Attributes().with(field, man)
        );
        data.put(
            table,
            new Attributes().with(field, woman)
        );
        data.delete(table, new Attributes().with(field, man));
        final List<Attributes> rest = Lists.newArrayList(
            data.iterate(table, new Conditions())
        );
        MatcherAssert.assertThat(
            rest.size(),
            Matchers.equalTo(1)
        );
        MatcherAssert.assertThat(
            rest.get(0).get(field).getS(),
            Matchers.equalTo(woman)
        );
    }

    @Test
    void updatesTableAttributes() throws Exception {
        final String table = "tests";
        final String key = "tid";
        final int number = 43;
        final String attr = "descr";
        final String value = "Dummy\n\t\u20ac text";
        final String updated = "Updated";
        final MkData data = new H2Data().with(
            table, new String[] {key}, attr
        );
        data.put(table, new Attributes().with(key, number).with(attr, value));
        data.update(
            table,
            new Attributes().with(key, number),
            new AttributeUpdates().with(attr, "something else")
        );
        data.update(
            table,
            new Attributes().with(key, number),
            new AttributeUpdates().with(attr, updated)
        );
        final Iterable<Attributes> result = data.iterate(
            table, new Conditions().with(key, Conditions.equalTo(number))
        );
        MatcherAssert.assertThat(
            result.iterator().next(),
            Matchers.hasEntry(
                Matchers.equalTo(attr),
                Matchers.equalTo(new AttributeValue(updated))
            )
        );
        MatcherAssert.assertThat(
            result,
            Matchers.iterableWithSize(1)
        );
    }

    @Test
    void fetchesWithComparison() throws Exception {
        final String table = "x12";
        final String key = "foo1";
        final String value = "bar2";
        final MkData data = new H2Data().with(table, new String[] {key}, value);
        data.put(table, new Attributes().with(key, "101").with(value, 0));
        data.put(table, new Attributes().with(key, "102").with(value, 1));
        MatcherAssert.assertThat(
            data.iterate(table, new Conditions()),
            Matchers.iterableWithSize(2)
        );
        MatcherAssert.assertThat(
            data.iterate(
                table,
                new Conditions().with(
                    value,
                    new Condition()
                        .withAttributeValueList(
                            new AttributeValue().withN("0")
                        )
                        .withComparisonOperator(ComparisonOperator.GT)
                )
            ).iterator().next().get(value).getN(),
            Matchers.equalTo("1")
        );
    }

}
