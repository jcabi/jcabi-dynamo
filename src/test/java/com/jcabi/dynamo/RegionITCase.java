/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.util.Iterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

/**
 * Integration case for {@link Region}.
 * @since 0.1
 */
final class RegionITCase {

    @BeforeEach
    void itTestCheck() {
        Assumptions.assumeFalse(System.getProperty("failsafe.port", "").isEmpty());
    }

    @Test
    void queriesFrameSize() throws Exception {
        final String name = RandomStringUtils.secure().nextAlphabetic(8);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String hash = RandomStringUtils.secure().nextAlphanumeric(10);
        for (int idx = 0; idx < 5; ++idx) {
            tbl.put(
                new Attributes()
                    .with(mock.hash(), hash)
                    .with(mock.range(), idx)
                    .with("some-attr", "val")
            );
        }
        MatcherAssert.assertThat(
            "should has size 5",
            tbl.frame()
                .where(mock.hash(), Conditions.equalTo(hash))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(5)
        );
    }

    @Test
    void scansFrameSize() throws Exception {
        final String name = RandomStringUtils.secure().nextAlphabetic(8);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String attr = RandomStringUtils.secure().nextAlphabetic(8);
        final String value = RandomStringUtils.secure().nextAlphanumeric(10);
        final String hash = RandomStringUtils.secure().nextAlphanumeric(10);
        for (int idx = 0; idx < 5; ++idx) {
            tbl.put(
                new Attributes()
                    .with(mock.hash(), hash)
                    .with(mock.range(), idx)
                    .with(attr, value)
            );
        }
        MatcherAssert.assertThat(
            "should has size 5",
            tbl.frame()
                .where(attr, Conditions.equalTo(value))
                .through(
                    new ScanValve()
                        .withLimit(10)
                        .withAttributeToGet(attr)
                ),
            Matchers.hasSize(5)
        );
    }

    @Test
    void readsAttributeFromItem() throws Exception {
        final String name = RandomStringUtils.secure().nextAlphabetic(8);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String attr = RandomStringUtils.secure().nextAlphabetic(8);
        final String value = RandomStringUtils.secure().nextAlphanumeric(10);
        tbl.put(
            new Attributes()
                .with(mock.hash(), "somehash")
                .with(mock.range(), 0)
                .with(attr, value)
        );
        MatcherAssert.assertThat(
            "should equal to attribute value",
            tbl.frame()
                .where(attr, Conditions.equalTo(value))
                .through(
                    new ScanValve()
                        .withLimit(10)
                        .withAttributeToGet(attr)
                )
                .iterator().next().get(attr).s(),
            Matchers.equalTo(value)
        );
    }

    @Test
    void updatesAndReadsModifiedAttribute() throws Exception {
        final String name = RandomStringUtils.secure().nextAlphabetic(8);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String attr = RandomStringUtils.secure().nextAlphabetic(8);
        final String value = RandomStringUtils.secure().nextAlphanumeric(10);
        final String hash = RandomStringUtils.secure().nextAlphanumeric(10);
        tbl.put(
            new Attributes()
                .with(mock.hash(), hash)
                .with(mock.range(), 0)
                .with(attr, value)
        );
        final Iterator<Item> items = tbl.frame()
            .where(attr, Conditions.equalTo(value))
            .through(
                new ScanValve()
                    .withLimit(10)
                    .withAttributeToGet(attr)
            ).iterator();
        final Item item = items.next();
        item.put(
            attr,
            AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("empty").build())
                .action(AttributeAction.PUT)
                .build()
        );
        MatcherAssert.assertThat(
            "should not equal to original value",
            tbl.frame()
                .where(mock.hash(), hash)
                .where(mock.range(), Conditions.equalTo(0))
                .through(new ScanValve())
                .iterator().next()
                .get(attr).s(),
            Matchers.not(Matchers.equalTo(value))
        );
    }

    @Test
    @Disabled
    void retrievesAttributesFromDynamo() throws Exception {
        final String name = RandomStringUtils.secure().nextAlphabetic(8);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final int idx = 10;
        final String hash = "7abc5cba";
        final String attr = "some-attribute";
        tbl.put(
            new Attributes()
                .with(mock.hash(), hash)
                .with(mock.range(), idx)
                .with(attr, "test-value")
        );
        MatcherAssert.assertThat(
            "should not retrieve something",
            tbl.frame()
                .where(mock.hash(), hash)
                .where(mock.range(), Conditions.equalTo(idx))
                .through(
                    new QueryValve()
                        .withAttributeToGet(attr)
                        .withConsistentRead(true)
                        .withLimit(50)
                )
                .iterator().next()
                .has("something"),
            Matchers.is(false)
        );
    }

}
