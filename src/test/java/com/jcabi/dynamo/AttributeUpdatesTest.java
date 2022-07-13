/**
 * Copyright (c) 2012-2017, jcabi.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 1) Redistributions of source code must retain the above
 * copyright notice, this list of conditions and the following
 * disclaimer. 2) Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided
 * with the distribution. 3) Neither the name of the jcabi.com nor
 * the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link AttributesUpdates}.
 * @author Mihai Andronache (amihaiemil@gmail.com)
 * @version $Id$
 * @since 0.22
 */
@SuppressWarnings("PMD.TooManyMethods")
public final class AttributeUpdatesTest {

    /**
     * AttributesUpdates can tell if it's empty or not.
     */
    @Test
    public void tellsIfEmptyOrNot() {
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(attr.isEmpty(), Matchers.is(Boolean.TRUE));
        MatcherAssert.assertThat(
            attr.with("testkey", Mockito.mock(AttributeValueUpdate.class))
                .isEmpty(),
            Matchers.is(Boolean.FALSE)
        );
    }

    /**
     * AttributesUpdates can add a new AttributeValueUpdate.
     */
    @Test
    public void addsAttributeValueUpdate() {
        MatcherAssert.assertThat(0, Matchers.is(0));
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(
            attr.with("testkey1", Mockito.mock(AttributeValueUpdate.class))
                .size(),
            Matchers.is(1)
        );
    }

    /**
     * AttributesUpdates can add a new AttributeValue.
     */
    @Test
    public void addsAttributeValue() {
        MatcherAssert.assertThat(0, Matchers.is(0));
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(
            attr.with("testkey2", Mockito.mock(AttributeValue.class)).size(),
            Matchers.is(1)
        );
    }

    /**
     * AttributesUpdates can add a new Object.
     */
    @Test
    public void addsObject() {
        MatcherAssert.assertThat(0, Matchers.is(0));
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(
            attr.with("testkey3", "value here").size(),
            Matchers.is(1)
        );
    }

    /**
     * AttributesUpdates can return its key Set.
     */
    @Test
    public void returnsKeySet() {
        final String firstkey = "key1";
        final AttributeUpdates attr = new AttributeUpdates()
            .with(firstkey, "valuediff").with("key2", "value2");
        MatcherAssert.assertThat(
            attr.keySet().size(),
            Matchers.is(2)
        );
        MatcherAssert.assertThat(
            attr.keySet().iterator().next(),
            Matchers.equalTo(firstkey)
        );
    }

    /**
     * AttributesUpdates can return its values.
     */
    @Test
    public void returnsValues() {
        final String firstvalue = "value3";
        final AttributeUpdates attr = new AttributeUpdates()
            .with("key3", firstvalue).with("key4", "value4");
        MatcherAssert.assertThat(
            attr.values().size(),
            Matchers.is(2)
        );
        MatcherAssert.assertThat(
            attr.values().iterator().next().getValue().getS(),
            Matchers.equalTo(firstvalue)
        );
    }

    /**
     * AttributesUpdates can return its entry Set.
     */
    @Test
    public void returnsEntries() {
        final AttributeUpdates attr = new AttributeUpdates()
            .with("key5", "value5").with("key6", "value7");
        MatcherAssert.assertThat(
            attr.entrySet().size(),
            Matchers.is(2)
        );
    }

    /**
     * AttributesUpdates can return an entry by the key.
     */
    @Test
    public void getsEntry() {
        final String key = "key10";
        final AttributeUpdates attr = new AttributeUpdates()
            .with(key, "value10");
        MatcherAssert.assertThat(
            attr.get(key),
            Matchers.notNullValue()
        );
    }

    /**
     * AttributesUpdates can tell if it contains a key.
     */
    @Test
    public void containsKey() {
        final String key = "key11";
        final AttributeUpdates attr = new AttributeUpdates()
            .with(key, "value11").with("otherkey1", "othervalue1");
        MatcherAssert.assertThat(
            attr.containsKey(key),
            Matchers.is(Boolean.TRUE)
        );
    }

    /**
     * AttributesUpdates can tell if it contains a value.
     */
    @Test
    public void containsValue() {
        final String value = "attrv";
        final AttributeUpdates attr = new AttributeUpdates()
            .with("attrkey", value).with("otherkey", "othervalue");
        MatcherAssert.assertThat(
            attr.containsValue(
                new AttributeValueUpdate(
                    new AttributeValue(value), AttributeAction.PUT
                )
            ),
            Matchers.is(Boolean.TRUE)
        );
    }

    /**
     * AttributesUpdates can return a string representation of itself.
     */
    @Test
    public void canTurnToString() {
        final AttributeUpdates attr = new AttributeUpdates()
            .with("onekey", "onevalue").with("secondkey", "secondvalue");
        MatcherAssert.assertThat(
            attr.toString(),
            Matchers.equalTo(
                StringUtils.join(
                    "onekey={Value: {S: onevalue,},Action: PUT}; ",
                    "secondkey={Value: {S: secondvalue,},Action: PUT}"
                )
            )
        );
    }

    /**
     * AttributesUpdates can add the content of another Map.
     */
    @Test
    public void addsMap() {
        final AttributeUpdates attr = new AttributeUpdates();
        MatcherAssert.assertThat(attr.size(), Matchers.is(0));
        MatcherAssert.assertThat(
            attr.with("testkey8", new AttributeUpdates().with("key", "value"))
                .size(),
            Matchers.is(1)
        );
    }

    /**
     * AttributesUpdates can throw exception when put is called.
     */
    @Test
    public void putThrowsException() {
        boolean passed;
        try {
            new AttributeUpdates().put(
                "key9", Mockito.mock(AttributeValueUpdate.class)
            );
            passed = false;
        } catch (final UnsupportedOperationException ex) {
            passed = true;
        }
        if (!passed) {
            Assertions.fail("#put should not be supported");
        }
    }

    /**
     * AttributesUpdates can throw exception when putAll is called.
     */
    @Test
    public void putAllThrowsException() {
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

    /**
     * AttributesUpdates can throw exception when remove is called.
     */
    @Test
    public void removeThrowsException() {
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

    /**
     * AttributesUpdates can throw exception when clear is called.
     */
    @Test
    public void clearThrowsException() {
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
