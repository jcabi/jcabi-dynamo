/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Test case for {@link Conditions}.
 * @since 0.1
 */
@SuppressWarnings("PMD.TooManyMethods")
final class ConditionsTest {

    @Test
    void worksAsMapWithCorrectKeySetSize() {
        MatcherAssert.assertThat(
            "should has size 1",
            new Conditions().with(
                "id", Condition.builder().build()
            ).keySet(),
            Matchers.hasSize(1)
        );
    }

    @Test
    void worksAsMapWithCorrectEntry() {
        final String name = "id";
        final Condition condition = Condition.builder().build();
        MatcherAssert.assertThat(
            "should has entry",
            new Conditions().with(name, condition),
            Matchers.hasEntry(name, condition)
        );
    }

    @Test
    void worksAsMapFromExistingMap() {
        final String name = "id";
        final Condition condition = Condition.builder().build();
        MatcherAssert.assertThat(
            "should has entry from existing map",
            new Conditions(new Conditions().with(name, condition)),
            Matchers.hasEntry(name, condition)
        );
    }

    @Test
    void reportsEmptyForNewInstance() {
        MatcherAssert.assertThat(
            "should not report non-empty for new instance",
            new Conditions().isEmpty(),
            Matchers.is(true)
        );
    }

    @Test
    void reportsNonEmptyAfterAdd() {
        MatcherAssert.assertThat(
            "should not report empty after adding condition",
            new Conditions()
                .with("\u00e4ttr", Condition.builder().build())
                .isEmpty(),
            Matchers.is(false)
        );
    }

    @Test
    void containsAddedKey() {
        MatcherAssert.assertThat(
            "should not miss added key",
            new Conditions()
                .with("k\u00e9y", Condition.builder().build())
                .containsKey("k\u00e9y"),
            Matchers.is(true)
        );
    }

    @Test
    void doesNotContainAbsentKey() {
        MatcherAssert.assertThat(
            "should not contain absent key",
            new Conditions()
                .with("pr\u00e9s", Condition.builder().build())
                .containsKey("n\u00f6pe"),
            Matchers.is(false)
        );
    }

    @Test
    void containsAddedValue() {
        final Condition cond = Condition.builder().build();
        MatcherAssert.assertThat(
            "should not miss added value",
            new Conditions()
                .with("n\u00e4me", cond)
                .containsValue(cond),
            Matchers.is(true)
        );
    }

    @Test
    void retrievesConditionByKey() {
        final Condition cond = Condition.builder().build();
        MatcherAssert.assertThat(
            "should not return wrong condition for key",
            new Conditions()
                .with("r\u00e9tr", cond)
                .get("r\u00e9tr"),
            Matchers.equalTo(cond)
        );
    }

    @Test
    void returnsCorrectSize() {
        MatcherAssert.assertThat(
            "should not return wrong size",
            new Conditions()
                .with("f\u00ecrst", Condition.builder().build())
                .with("s\u00e9cond", Condition.builder().build())
                .size(),
            Matchers.equalTo(2)
        );
    }

    @Test
    void throwsOnPut() {
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> new Conditions()
                .put("p\u00fct", Condition.builder().build())
        );
    }

    @Test
    void throwsOnRemove() {
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> new Conditions().remove("r\u00e9m")
        );
    }

    @Test
    void throwsOnPutAll() {
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> new Conditions().putAll(
                Collections.singletonMap(
                    "b\u00fclk", Condition.builder().build()
                )
            )
        );
    }

    @Test
    void throwsOnClear() {
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> new Conditions().clear()
        );
    }

    @Test
    void createsEqualConditionForLong() {
        MatcherAssert.assertThat(
            "should not miss numeric attribute for Long",
            Conditions.equalTo(42L)
                .attributeValueList().get(0).n(),
            Matchers.equalTo("42")
        );
    }

    @Test
    void createsEqualConditionForInteger() {
        MatcherAssert.assertThat(
            "should not miss numeric attribute for Integer",
            Conditions.equalTo(7)
                .attributeValueList().get(0).n(),
            Matchers.equalTo("7")
        );
    }

    @Test
    void createsEqualConditionForObject() {
        MatcherAssert.assertThat(
            "should not miss string attribute for Object",
            Conditions.equalTo("v\u00e4lue")
                .attributeValueList().get(0).s(),
            Matchers.equalTo("v\u00e4lue")
        );
    }

    @Test
    void mergesWithAttributeMap() {
        MatcherAssert.assertThat(
            "should not fail merging attribute map",
            new Conditions().withAttributes(
                Collections.singletonMap(
                    "m\u00e9rge",
                    AttributeValue.builder().s("v\u00e4l").build()
                )
            ).keySet(),
            Matchers.hasSize(1)
        );
    }

    @Test
    void combinesConditionsViaMap() {
        MatcherAssert.assertThat(
            "should not fail combining conditions via map",
            new Conditions().with(
                Collections.singletonMap(
                    "c\u00f6mb", Condition.builder().build()
                )
            ).keySet(),
            Matchers.hasSize(1)
        );
    }

    @Test
    void addsConditionViaObject() {
        MatcherAssert.assertThat(
            "should not fail adding condition via object",
            new Conditions().with("n\u00e4me", (Object) "v\u00e4lue"),
            Matchers.hasKey("n\u00e4me")
        );
    }

    @Test
    void formatsConditionsToString() {
        MatcherAssert.assertThat(
            "should not produce string without attribute name",
            new Conditions()
                .with("r\u00e4nge", Conditions.equalTo("v\u00e4l"))
                .toString(),
            Matchers.containsString("r\u00e4nge")
        );
    }

}
