/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.immutable.ArrayMap;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;

/**
 * Test case for {@link Attributes}.
 * @since 0.1
 */
final class AttributesTest {

    @Test
    void worksAsMapWithCorrectKeySetSize() {
        MatcherAssert.assertThat(
            "should has size 1",
            new Attributes().with(
                "id",
                AttributeValue.builder().s("some text value").build()
            ).keySet(),
            Matchers.hasSize(1)
        );
    }

    @Test
    void worksAsMapWithCorrectEntry() {
        final String attr = "id";
        final AttributeValue value = AttributeValue.builder().s("some text value").build();
        MatcherAssert.assertThat(
            "should has some entry",
            new Attributes().with(attr, value),
            Matchers.hasEntry(attr, value)
        );
    }

    @Test
    void worksAsMapFromExistingMap() {
        final String attr = "id";
        final AttributeValue value = AttributeValue.builder().s("some text value").build();
        MatcherAssert.assertThat(
            "should has some text value",
            new Attributes(new Attributes().with(attr, value)),
            Matchers.hasEntry(attr, value)
        );
    }

    @Test
    void buildsExpectedKeys() {
        final String attr = "attr-13";
        final String value = "some value \u20ac";
        MatcherAssert.assertThat(
            "should has 'some value \u20ac'",
            new Attributes().with(attr, value).asKeys(),
            Matchers.hasEntry(
                attr,
                ExpectedAttributeValue.builder().value(
                    AttributeValue.builder().s(value).build()
                ).build()
            )
        );
    }

    @Test
    void filtersOutUnnecessaryKeys() {
        MatcherAssert.assertThat(
            "should be empty match",
            new Attributes()
                .with("first", "test-1")
                .with("second", "test-2")
                .only(Collections.singletonList("never"))
                .keySet(),
            Matchers.empty()
        );
    }

    @Test
    void caseSensitiveWithDifferentCase() {
        MatcherAssert.assertThat(
            "should has size 2",
            new Attributes().with(
                new ArrayMap<String, AttributeValue>()
                    .with("Gamma", AttributeValue.builder().s("").build())
                    .with("gAMma", AttributeValue.builder().s("").build())
            ).keySet(),
            Matchers.hasSize(2)
        );
    }

    @Test
    void caseSensitivePreservesKeys() {
        final String first = "Alpha";
        final String second = "AlPha";
        MatcherAssert.assertThat(
            "should has keys 'Alpha', 'AlPha'",
            new Attributes()
                .with(first, "val-1")
                .with(second, "val-2"),
            Matchers.allOf(
                Matchers.hasKey(first),
                Matchers.hasKey(second)
            )
        );
    }

    @Test
    void caseSensitiveFiltersCorrectly() {
        final String third = "Beta";
        MatcherAssert.assertThat(
            "should has key 'Beta'",
            new Attributes()
                .with(third, "some text to use")
                .only(Collections.singletonList(third)),
            Matchers.hasKey(third)
        );
    }

}
