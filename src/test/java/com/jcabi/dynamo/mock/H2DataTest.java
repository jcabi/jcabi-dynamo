/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;

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
            "should stores and reads attributes",
            data.iterate(
                table, new Conditions().with(key, Conditions.equalTo(number))
            ).iterator().next(),
            Matchers.hasEntry(
                Matchers.equalTo(attr),
                Matchers.equalTo(AttributeValue.builder().s(value).build())
            )
        );
    }

    @Test
    @Disabled
    void storesToFileAndExists(@TempDir final Path temp) throws Exception {
        final File file = temp.resolve("foo.txt").toFile();
        final String table = "tbl";
        final String key = "user";
        final MkData data = new H2Data(file).with(
            table, new String[] {key}
        );
        data.put(table, new Attributes().with(key, "x2"));
        MatcherAssert.assertThat(
            "should exists the file",
            file.exists(),
            Matchers.is(true)
        );
    }

    @Test
    void createsManyTables() throws Exception {
        MatcherAssert.assertThat(
            "should create two tables",
            new H2Data()
                .with("firsttable", new String[] {"firstid"}, "test")
                .with("secondtable", new String[]{"secondid"}),
            Matchers.notNullValue()
        );
    }

    @Test
    void createsTablesWithLongNames() throws Exception {
        MatcherAssert.assertThat(
            "should create table with long name",
            new H2Data().with(
                Joiner.on("").join(Collections.nCopies(40, "X")),
                new String[]{"k1"}
            ),
            Matchers.notNullValue()
        );
    }

    @Test
    void supportsTableNamesWithIllegalCharacters() throws Exception {
        MatcherAssert.assertThat(
            "should support illegal characters in table name",
            new H2Data().with(".-.", new String[]{"pk"}),
            Matchers.notNullValue()
        );
    }

    @Test
    @Disabled
    void supportsColumnNamesWithIllegalCharacters() throws Exception {
        MatcherAssert.assertThat(
            "should support illegal characters in column name",
            new H2Data().with(
                "test", new String[] {"0-.col.-0"}
            ),
            Matchers.notNullValue()
        );
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
        MatcherAssert.assertThat(
            "should delete Kevin and keep only Helen",
            Lists.newArrayList(
                data.iterate(table, new Conditions())
            ),
            Matchers.contains(
                Matchers.hasEntry(
                    Matchers.equalTo(field),
                    Matchers.equalTo(
                        AttributeValue.builder().s(woman).build()
                    )
                )
            )
        );
    }

    @Test
    void updatesTableAttributes() throws Exception {
        final String table = "tests";
        final String key = "tid";
        final int number = 43;
        final String attr = "descr";
        final String updated = "Updated";
        final MkData data = new H2Data().with(
            table, new String[] {key}, attr
        );
        data.put(
            table,
            new Attributes()
                .with(key, number)
                .with(attr, "Dummy\n\t\u20ac text")
        );
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
        MatcherAssert.assertThat(
            "should update to single correct entry",
            Lists.newArrayList(
                data.iterate(
                    table,
                    new Conditions().with(key, Conditions.equalTo(number))
                )
            ),
            Matchers.contains(
                Matchers.hasEntry(
                    Matchers.equalTo(attr),
                    Matchers.equalTo(
                        AttributeValue.builder().s(updated).build()
                    )
                )
            )
        );
    }

    @Test
    void fetchesAllWithoutConditions() throws Exception {
        final String table = "x12";
        final String key = "foo1";
        final String value = "bar2";
        final MkData data = new H2Data().with(
            table, new String[] {key}, value
        );
        data.put(table, new Attributes().with(key, "101").with(value, 0));
        data.put(table, new Attributes().with(key, "102").with(value, 1));
        MatcherAssert.assertThat(
            "should iterable with size 2",
            data.iterate(table, new Conditions()),
            Matchers.iterableWithSize(2)
        );
    }

    @Test
    void fetchesWithComparison() throws Exception {
        final String table = "x12";
        final String key = "foo1";
        final String value = "bar2";
        final MkData data = new H2Data().with(
            table, new String[] {key}, value
        );
        data.put(table, new Attributes().with(key, "101").with(value, 0));
        data.put(table, new Attributes().with(key, "102").with(value, 1));
        MatcherAssert.assertThat(
            "should equal to '1'",
            data.iterate(
                table,
                new Conditions().with(
                    value,
                    Condition.builder()
                        .attributeValueList(
                            AttributeValue.builder().n("0").build()
                        )
                        .comparisonOperator(ComparisonOperator.GT)
                        .build()
                )
            ).iterator().next().get(value).n(),
            Matchers.equalTo("1")
        );
    }

}
