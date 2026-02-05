/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

/**
 * Test case for {@link AttributesUpdates}.
 * @since 0.22
 */
@SuppressWarnings("PMD.TooManyMethods")
final class AttributeUpdatesTest {

    @Test
    void tellsIfEmpty() {
        MatcherAssert.assertThat(
            "should be true",
            new AttributeUpdates().isEmpty(),
            Matchers.is(Boolean.TRUE)
        );
    }

    @Test
    void tellsIfNotEmpty() {
        MatcherAssert.assertThat(
            "should be false",
            new AttributeUpdates()
                .with("testkey", AttributeValueUpdate.builder().build())
                .isEmpty(),
            Matchers.is(Boolean.FALSE)
        );
    }

    @Test
    void addsAttributeValueUpdate() {
        MatcherAssert.assertThat(
            "should be 1",
            new AttributeUpdates()
                .with("testkey1", AttributeValueUpdate.builder().build())
                .size(),
            Matchers.is(1)
        );
    }

    @Test
    void addsAttributeValue() {
        MatcherAssert.assertThat(
            "should be 1",
            new AttributeUpdates()
                .with("testkey2", AttributeValue.builder().s("mock").build())
                .size(),
            Matchers.is(1)
        );
    }

    @Test
    void addsObject() {
        MatcherAssert.assertThat(
            "should be 1",
            new AttributeUpdates()
                .with("testkey3", "value here")
                .size(),
            Matchers.is(1)
        );
    }

    @Test
    void returnsKeySetSize() {
        MatcherAssert.assertThat(
            "should be 2",
            new AttributeUpdates()
                .with("key1", "valuediff")
                .with("key2", "value2")
                .keySet()
                .size(),
            Matchers.is(2)
        );
    }

    @Test
    void returnsKeySetContent() {
        final String firstkey = "key1";
        MatcherAssert.assertThat(
            "should be equal to 'key1'",
            new AttributeUpdates()
                .with(firstkey, "valuediff")
                .with("key2", "value2")
                .keySet()
                .iterator()
                .next(),
            Matchers.equalTo(firstkey)
        );
    }

    @Test
    void returnsValuesSize() {
        MatcherAssert.assertThat(
            "should be 2",
            new AttributeUpdates()
                .with("key3", "value3")
                .with("key4", "value4")
                .values()
                .size(),
            Matchers.is(2)
        );
    }

    @Test
    void returnsValuesContent() {
        final String firstvalue = "value3";
        MatcherAssert.assertThat(
            "should be equal to 'value3'",
            new AttributeUpdates()
                .with("key3", firstvalue)
                .with("key4", "value4")
                .values()
                .iterator()
                .next()
                .value()
                .s(),
            Matchers.equalTo(firstvalue)
        );
    }

    @Test
    void returnsEntries() {
        MatcherAssert.assertThat(
            "should be 2",
            new AttributeUpdates()
                .with("key5", "value5")
                .with("key6", "value7")
                .entrySet()
                .size(),
            Matchers.is(2)
        );
    }

    @Test
    void getsEntry() {
        final String key = "key10";
        MatcherAssert.assertThat(
            "should be not null",
            new AttributeUpdates()
                .with(key, "value10")
                .get(key),
            Matchers.notNullValue()
        );
    }

    @Test
    void containsKey() {
        final String key = "key11";
        MatcherAssert.assertThat(
            "should contains key 'key11'",
            new AttributeUpdates()
                .with(key, "value11")
                .with("otherkey1", "othervalue1")
                .containsKey(key),
            Matchers.is(Boolean.TRUE)
        );
    }

    @Test
    void containsValue() {
        final String value = "attrv";
        MatcherAssert.assertThat(
            "should contains value 'attrv'",
            new AttributeUpdates()
                .with("attrkey", value)
                .with("otherkey", "othervalue")
                .containsValue(
                    AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s(value).build())
                        .action(AttributeAction.PUT)
                        .build()
                ),
            Matchers.is(Boolean.TRUE)
        );
    }

    @Test
    void canTurnToString() {
        MatcherAssert.assertThat(
            "should contain key names",
            new AttributeUpdates()
                .with("onekey", "onevalue")
                .with("secondkey", "secondvalue")
                .toString(),
            Matchers.allOf(
                Matchers.containsString("onekey="),
                Matchers.containsString("secondkey=")
            )
        );
    }

    @Test
    void addsMapReportsEmptySize() {
        MatcherAssert.assertThat(
            "should be 0",
            new AttributeUpdates().size(),
            Matchers.is(0)
        );
    }

    @Test
    void addsMap() {
        MatcherAssert.assertThat(
            "should be 1",
            new AttributeUpdates()
                .with("testkey8", new AttributeUpdates().with("key", "value"))
                .size(),
            Matchers.is(1)
        );
    }

    @Test
    void putThrowsException() {
        boolean passed;
        try {
            new AttributeUpdates().put(
                "key9", AttributeValueUpdate.builder().build()
            );
            passed = false;
        } catch (final UnsupportedOperationException ex) {
            passed = true;
        }
        if (!passed) {
            Assertions.fail("#put should not be supported");
        }
    }

    @Test
    void putAllThrowsException() {
        boolean passed;
        try {
            new AttributeUpdates().putAll(new AttributeUpdates());
            passed = false;
        } catch (final UnsupportedOperationException ex) {
            passed = true;
        }
        if (!passed) {
            Assertions.fail("#putAll should not be supported.");
        }
    }

    @Test
    void removeThrowsException() {
        boolean passed;
        try {
            new AttributeUpdates().remove("object to remove");
            passed = false;
        } catch (final UnsupportedOperationException ex) {
            passed = true;
        }
        if (!passed) {
            Assertions.fail("#remove should not be supported.");
        }
    }

    @Test
    void clearThrowsException() {
        boolean passed;
        try {
            new AttributeUpdates().clear();
            passed = false;
        } catch (final UnsupportedOperationException ex) {
            passed = true;
        }
        if (!passed) {
            Assertions.fail("#clear should not be supported.");
        }
    }
}
