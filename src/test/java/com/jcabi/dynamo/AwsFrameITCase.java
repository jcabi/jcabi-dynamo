/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Tv;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration case for {@link AwsFrame}.
 * @since 0.21
 */
final class AwsFrameITCase {

    @BeforeEach
    void itTestCheck() {
        Assumptions.assumeFalse(System.getProperty("failsafe.port", "").isEmpty());
    }

    @Test
    void calculatesItems() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String hash = "hello";
        final Attributes attrs = new Attributes().with(mock.hash(), hash);
        for (int idx = 0; idx < Tv.TEN; ++idx) {
            tbl.put(attrs.with(mock.range(), idx));
        }
        MatcherAssert.assertThat(
            "should equal to 10",
            tbl.frame()
                .through(new ScanValve().withLimit(1))
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
        MatcherAssert.assertThat(
            "should equal to false",
            tbl.frame()
                .through(new ScanValve().withLimit(1))
                .isEmpty(),
            Matchers.equalTo(false)
        );
        MatcherAssert.assertThat(
            "should equal to 10",
            tbl.frame()
                .through(new ScanValve().withLimit(Tv.HUNDRED))
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
        MatcherAssert.assertThat(
            "should equal to 10",
            tbl.frame()
                .through(new QueryValve().withLimit(1))
                .where(mock.hash(), hash)
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
        MatcherAssert.assertThat(
            "should equal to 10",
            tbl.frame()
                .through(new QueryValve().withLimit(Tv.HUNDRED))
                .where(mock.hash(), hash)
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
    }

}
