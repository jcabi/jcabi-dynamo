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
            "should has size 1",
            tbl.frame()
                .where("room", Conditions.equalTo(0))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(1)
        );
        MatcherAssert.assertThat(
            "should equals to 0",
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
