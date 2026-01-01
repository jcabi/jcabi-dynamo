/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.Condition;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Conditions}.
 * @since 0.1
 */
@SuppressWarnings("PMD.UseConcurrentHashMap")
final class ConditionsTest {

    @Test
    void workAsMapOfConditions() {
        final String name = "id";
        final Condition condition = new Condition();
        final Map<String, Condition> conds = new Conditions()
            .with(name, condition);
        MatcherAssert.assertThat("should has size 1", conds.keySet(), Matchers.hasSize(1));
        MatcherAssert.assertThat("should has entry", conds, Matchers.hasEntry(name, condition));
        MatcherAssert.assertThat(
            "should has entry",
            new Conditions(conds),
            Matchers.hasEntry(name, condition)
        );
    }

}
