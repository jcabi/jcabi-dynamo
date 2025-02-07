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
import java.util.Iterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration case with DynamoDB Local.
 *
 * @since 0.23
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
final class DynamodbLocalITCase {

    @BeforeEach
    void itTestCheck() {
        Assumptions.assumeFalse(
            System.getProperty("failsafe.ddl.port", "").isEmpty()
                || System.getProperty("failsafe.ddl.key", "").isEmpty()
                || System.getProperty("failsafe.ddl.secret", "").isEmpty(),
            "DynamoDbLocal is not up and running, that's why this test is skipped"
        );
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    void worksWithAmazon() throws Exception {
        final Table tbl = new Region.Simple(
            new Credentials.Direct(
                new Credentials.Simple(
                    System.getProperty("failsafe.ddl.key"),
                    System.getProperty("failsafe.ddl.secret")
                ),
                Integer.parseInt(System.getProperty("failsafe.ddl.port"))
            )
        ).table("talks");
        for (int idx = 0; idx < Tv.FIVE; ++idx) {
            tbl.put(
                new Attributes()
                    .with("room", idx)
                    .with("title", RandomStringUtils.randomAlphanumeric(Tv.TEN))
            );
        }
        MatcherAssert.assertThat(
            tbl.frame()
                .where("room", Conditions.equalTo(0))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(1)
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .where("room", Conditions.equalTo(0))
                .through(new ScanValve())
                .iterator().next()
                .get("room")
                .getN(),
            Matchers.equalTo("0")
        );
        final Iterator<Item> items = tbl.frame().iterator();
        items.next();
        items.remove();
    }

}
