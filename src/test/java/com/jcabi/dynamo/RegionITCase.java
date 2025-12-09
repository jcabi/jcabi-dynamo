/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.jcabi.aspects.Tv;
import java.util.Iterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void worksWithAmazon() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String attr = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final String value = RandomStringUtils.randomAlphanumeric(Tv.TEN);
        final String hash = RandomStringUtils.randomAlphanumeric(Tv.TEN);
        for (int idx = 0; idx < Tv.FIVE; ++idx) {
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
                .where(mock.hash(), Conditions.equalTo(hash))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(Tv.FIVE)
        );
        final Frame frame = tbl.frame()
            .where(attr, Conditions.equalTo(value))
            .through(
                new ScanValve()
                    .withLimit(Tv.TEN)
                    .withAttributeToGet(attr)
            );
        MatcherAssert.assertThat("should has size 5", frame, Matchers.hasSize(Tv.FIVE));
        final Iterator<Item> items = frame.iterator();
        final Item item = items.next();
        final int range = Integer.parseInt(item.get(mock.range()).getN());
        MatcherAssert.assertThat(
            "should equal to random alphanumeric value",
            item.get(attr).getS(),
            Matchers.equalTo(value)
        );
        item.put(
            attr,
            new AttributeValueUpdate(
                new AttributeValue("empty"),
                AttributeAction.PUT
            )
        );
        MatcherAssert.assertThat(
            "should not equal to random alphanumeric value",
            tbl.frame()
                .where(mock.hash(), hash)
                .where(mock.range(), Conditions.equalTo(range))
                .through(new ScanValve())
                .iterator().next()
                .get(attr).getS(),
            Matchers.not(Matchers.equalTo(value))
        );
        items.remove();
    }

    @Test
    @Disabled
    void retrievesAttributesFromDynamo() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final int idx = Tv.TEN;
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
                        .withLimit(Tv.FIFTY)
                )
                .iterator().next()
                .has("something"),
            Matchers.is(false)
        );
    }

}
