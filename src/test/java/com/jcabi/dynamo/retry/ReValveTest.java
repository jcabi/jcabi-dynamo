/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.dynamo.Credentials;
import com.jcabi.dynamo.Dosage;
import com.jcabi.dynamo.Valve;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Test case for {@link ReValve}.
 * @since 0.9
 */
final class ReValveTest {

    @Test
    void delegatesFetchAndWrapsInReDosage() throws Exception {
        MatcherAssert.assertThat(
            "does not wrap fetch result in ReDosage",
            new ReValve(
                new Valve() {
                    @Override
                    public Dosage fetch(
                        final Credentials credentials,
                        final String table,
                        final Map<String, Condition> conditions,
                        final Collection<String> keys) {
                        return new Dosage.Empty();
                    }

                    @Override
                    public int count(
                        final Credentials credentials,
                        final String table,
                        final Map<String, Condition> conditions) {
                        return 0;
                    }
                }
            ).fetch(
                Credentials.TEST,
                "t\u00e9st",
                Collections.emptyMap(),
                Collections.emptyList()
            ),
            Matchers.instanceOf(ReDosage.class)
        );
    }

    @Test
    void delegatesCountToOrigin() throws Exception {
        MatcherAssert.assertThat(
            "does not delegate count to origin valve",
            new ReValve(
                new Valve() {
                    @Override
                    public Dosage fetch(
                        final Credentials credentials,
                        final String table,
                        final Map<String, Condition> conditions,
                        final Collection<String> keys) {
                        return new Dosage.Empty();
                    }

                    @Override
                    public int count(
                        final Credentials credentials,
                        final String table,
                        final Map<String, Condition> conditions) {
                        return 42;
                    }
                }
            ).count(
                Credentials.TEST,
                "t\u00e9st",
                Collections.emptyMap()
            ),
            Matchers.equalTo(42)
        );
    }
}
