/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.jcabi.immutable.ArrayMap;
import java.util.Collections;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Attributes}.
 * @since 0.1
 */
@SuppressWarnings("PMD.UseConcurrentHashMap")
final class AttributesTest {

    @Test
    void workAsMapOfAttributes() {
        final String attr = "id";
        final AttributeValue value = new AttributeValue("some text value");
        final Map<String, AttributeValue> attrs = new Attributes()
            .with(attr, value);
        MatcherAssert.assertThat(attrs.keySet(), Matchers.hasSize(1));
        MatcherAssert.assertThat(attrs, Matchers.hasEntry(attr, value));
        MatcherAssert.assertThat(
            new Attributes(attrs),
            Matchers.hasEntry(attr, value)
        );
    }

    @Test
    void buildsExpectedKeys() {
        final String attr = "attr-13";
        final String value = "some value \u20ac";
        MatcherAssert.assertThat(
            new Attributes().with(attr, value).asKeys(),
            Matchers.hasEntry(
                attr,
                new ExpectedAttributeValue(new AttributeValue(value))
            )
        );
    }

    @Test
    void filtersOutUnnecessaryKeys() {
        MatcherAssert.assertThat(
            new Attributes()
                .with("first", "test-1")
                .with("second", "test-2")
                .only(Collections.singletonList("never"))
                .keySet(),
            Matchers.empty()
        );
    }

    @Test
    void caseSensitive() {
        final String first = "Alpha";
        final String second = "AlPha";
        final String third = "Beta";
        MatcherAssert.assertThat(
            new Attributes().with(
                new ArrayMap<String, AttributeValue>()
                    .with("Gamma", new AttributeValue(""))
                    .with("gAMma", new AttributeValue(""))
            ).keySet(),
            Matchers.hasSize(2)
        );
        MatcherAssert.assertThat(
            new Attributes()
                .with(first, "val-1")
                .with(second, "val-2"),
            Matchers.allOf(
                Matchers.hasKey(first),
                Matchers.hasKey(second)
            )
        );
        MatcherAssert.assertThat(
            new Attributes()
                .with(third, "some text to use")
                .only(Collections.singletonList(third)),
            Matchers.hasKey(third)
        );
    }

}
