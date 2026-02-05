/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

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
    void queriesWithLimit() throws Exception {
        final String col = "room";
        final Table tbl = new Region.Simple(
            new Credentials.Direct(
                new Credentials.Simple(
                    System.getProperty("failsafe.ddl.key"),
                    System.getProperty("failsafe.ddl.secret")
                ),
                Integer.parseInt(System.getProperty("failsafe.ddl.port"))
            )
        ).table("talks");
        for (int idx = 0; idx < 5; ++idx) {
            tbl.put(
                new Attributes()
                    .with(col, idx)
                    .with("title", RandomStringUtils.secure().nextAlphanumeric(10))
            );
        }
        MatcherAssert.assertThat(
            "should has size 1",
            tbl.frame()
                .where(col, Conditions.equalTo(0))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(1)
        );
    }

    @Test
    void scansAndReadsValue() throws Exception {
        final String col = "room";
        final Table tbl = new Region.Simple(
            new Credentials.Direct(
                new Credentials.Simple(
                    System.getProperty("failsafe.ddl.key"),
                    System.getProperty("failsafe.ddl.secret")
                ),
                Integer.parseInt(System.getProperty("failsafe.ddl.port"))
            )
        ).table("talks");
        tbl.put(
            new Attributes()
                .with(col, 0)
                .with("title", RandomStringUtils.secure().nextAlphanumeric(10))
        );
        MatcherAssert.assertThat(
            "should equals to 0",
            tbl.frame()
                .where(col, Conditions.equalTo(0))
                .through(new ScanValve())
                .iterator().next()
                .get(col)
                .n(),
            Matchers.equalTo("0")
        );
    }

}
