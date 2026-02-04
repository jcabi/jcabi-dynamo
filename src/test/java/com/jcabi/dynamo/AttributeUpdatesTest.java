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
    void tellsIfEmptyOrNot() {
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat("should be true", attr.isEmpty(), Matchers.is(Boolean.TRUE));
        MatcherAssert.assertThat(
            "should be false",
            attr.with("testkey", AttributeValueUpdate.builder().build())
                .isEmpty(),
            Matchers.is(Boolean.FALSE)
        );
    }

    @Test
    void addsAttributeValueUpdate() {
        MatcherAssert.assertThat("should be 0", 0, Matchers.is(0));
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(
            "should be 1",
            attr.with("testkey1", AttributeValueUpdate.builder().build())
                .size(),
            Matchers.is(1)
        );
    }

    @Test
    void addsAttributeValue() {
        MatcherAssert.assertThat("should be 0", 0, Matchers.is(0));
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(
            "should be 1",
            attr.with("testkey2", AttributeValue.builder().s("mock").build()).size(),
            Matchers.is(1)
        );
    }

    @Test
    void addsObject() {
        MatcherAssert.assertThat("should be 0", 0, Matchers.is(0));
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(
            "should be 1",
            attr.with("testkey3", "value here").size(),
            Matchers.is(1)
        );
    }

    @Test
    void returnsKeySet() {
        final String firstkey = "key1";
        final AttributeUpdates attr = new AttributeUpdates()
            .with(firstkey, "valuediff").with("key2", "value2");
        MatcherAssert.assertThat(
            "should be 2",
            attr.keySet().size(),
            Matchers.is(2)
        );
        MatcherAssert.assertThat(
            "should be equal to 'key1'",
            attr.keySet().iterator().next(),
            Matchers.equalTo(firstkey)
        );
    }

    @Test
    void returnsValues() {
        final String firstvalue = "value3";
        final AttributeUpdates attr = new AttributeUpdates()
            .with("key3", firstvalue).with("key4", "value4");
        MatcherAssert.assertThat(
            "should be 2",
            attr.values().size(),
            Matchers.is(2)
        );
        MatcherAssert.assertThat(
            "should be equal to 'value3'",
            attr.values().iterator().next().value().s(),
            Matchers.equalTo(firstvalue)
        );
    }

    @Test
    void returnsEntries() {
        final AttributeUpdates attr = new AttributeUpdates()
            .with("key5", "value5").with("key6", "value7");
        MatcherAssert.assertThat(
            "should be 2",
            attr.entrySet().size(),
            Matchers.is(2)
        );
    }

    @Test
    void getsEntry() {
        final String key = "key10";
        final AttributeUpdates attr = new AttributeUpdates()
            .with(key, "value10");
        MatcherAssert.assertThat(
            "should be not null",
            attr.get(key),
            Matchers.notNullValue()
        );
    }

    @Test
    void containsKey() {
        final String key = "key11";
        final AttributeUpdates attr = new AttributeUpdates()
            .with(key, "value11").with("otherkey1", "othervalue1");
        MatcherAssert.assertThat(
            "should contains key 'key11'",
            attr.containsKey(key),
            Matchers.is(Boolean.TRUE)
        );
    }

    @Test
    void containsValue() {
        final String value = "attrv";
        final AttributeUpdates attr = new AttributeUpdates()
            .with("attrkey", value).with("otherkey", "othervalue");
        MatcherAssert.assertThat(
            "should contains value 'attrv'",
            attr.containsValue(
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
        final AttributeUpdates attr = new AttributeUpdates()
            .with("onekey", "onevalue").with("secondkey", "secondvalue");
        MatcherAssert.assertThat(
            "should contain key names",
            attr.toString(),
            Matchers.allOf(
                Matchers.containsString("onekey="),
                Matchers.containsString("secondkey=")
            )
        );
    }

    @Test
    void addsMap() {
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat("should be 0", attr.size(), Matchers.is(0));
        MatcherAssert.assertThat(
            "should be 1",
            attr.with("testkey8", new AttributeUpdates().with("key", "value"))
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
