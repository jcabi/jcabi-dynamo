/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Tv;
import java.util.Iterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration case for {@link AwsIterator}.
 * @since 0.16.2
 */
final class AwsIteratorITCase {

    @BeforeEach
    void itTestCheck() {
        Assumptions.assumeFalse(System.getProperty("failsafe.port", "").isEmpty());
    }

    @Test
    void iteratesItems() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        tbl.put(
            new Attributes()
                .with(mock.hash(), "test")
                .with(mock.range(), 1L)
        );
        MatcherAssert.assertThat(
            "should has size 1",
            tbl.frame(),
            Matchers.hasSize(1)
        );
    }

    @Test
    void iteratesItemsAndDeletes() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final Attributes attrs = new Attributes().with(mock.range(), 1L);
        for (int idx = 0; idx < Tv.SIX; ++idx) {
            tbl.put(attrs.with(mock.hash(), String.format("i%d", idx)));
        }
        final Iterator<Item> items = tbl.frame().iterator();
        int cnt = 0;
        while (items.hasNext()) {
            items.next();
            items.remove();
            ++cnt;
            if (cnt > Tv.HUNDRED) {
                throw new IllegalStateException("too many items");
            }
        }
        MatcherAssert.assertThat(
            "should has size 0",
            tbl.frame(),
            Matchers.hasSize(0)
        );
    }

}
