/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Test case for {@link Conditions}.
 * @since 0.1
 */
final class ConditionsTest {

    @Test
    void worksAsMapWithCorrectKeySetSize() {
        MatcherAssert.assertThat(
            "should has size 1",
            new Conditions().with(
                "id", Condition.builder().build()
            ).keySet(),
            Matchers.hasSize(1)
        );
    }

    @Test
    void worksAsMapWithCorrectEntry() {
        final String name = "id";
        final Condition condition = Condition.builder().build();
        MatcherAssert.assertThat(
            "should has entry",
            new Conditions().with(name, condition),
            Matchers.hasEntry(name, condition)
        );
    }

    @Test
    void worksAsMapFromExistingMap() {
        final String name = "id";
        final Condition condition = Condition.builder().build();
        MatcherAssert.assertThat(
            "should has entry from existing map",
            new Conditions(new Conditions().with(name, condition)),
            Matchers.hasEntry(name, condition)
        );
    }

}
