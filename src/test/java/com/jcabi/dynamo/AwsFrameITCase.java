/*
 * Copyright (c) 2012-2025 Yegor Bugayenko
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
            tbl.frame()
                .through(new ScanValve().withLimit(1))
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .through(new ScanValve().withLimit(1))
                .isEmpty(),
            Matchers.equalTo(false)
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .through(new ScanValve().withLimit(Tv.HUNDRED))
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .through(new QueryValve().withLimit(1))
                .where(mock.hash(), hash)
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .through(new QueryValve().withLimit(Tv.HUNDRED))
                .where(mock.hash(), hash)
                .size(),
            Matchers.equalTo(Tv.TEN)
        );
    }

}
