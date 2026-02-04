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
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void worksWithAmazon() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(8);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String attr = RandomStringUtils.randomAlphabetic(8);
        final String value = RandomStringUtils.randomAlphanumeric(10);
        final String hash = RandomStringUtils.randomAlphanumeric(10);
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
                .where(mock.hash(), Conditions.equalTo(hash))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(5)
        );
        final Frame frame = tbl.frame()
            .where(attr, Conditions.equalTo(value))
            .through(
                new ScanValve()
                    .withLimit(10)
                    .withAttributeToGet(attr)
            );
        MatcherAssert.assertThat("should has size 5", frame, Matchers.hasSize(5));
        final Iterator<Item> items = frame.iterator();
        final Item item = items.next();
        final int range = Integer.parseInt(item.get(mock.range()).n());
        MatcherAssert.assertThat(
            "should equal to random alphanumeric value",
            item.get(attr).s(),
            Matchers.equalTo(value)
        );
        item.put(
            attr,
            AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("empty").build())
                .action(AttributeAction.PUT)
                .build()
        );
        MatcherAssert.assertThat(
            "should not equal to random alphanumeric value",
            tbl.frame()
                .where(mock.hash(), hash)
                .where(mock.range(), Conditions.equalTo(range))
                .through(new ScanValve())
                .iterator().next()
                .get(attr).s(),
            Matchers.not(Matchers.equalTo(value))
        );
        items.remove();
    }

    @Test
    @Disabled
    void retrievesAttributesFromDynamo() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(8);
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
