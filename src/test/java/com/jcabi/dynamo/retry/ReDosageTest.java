/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.dynamo.Dosage;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Test case for {@link ReDosage}.
 * @since 0.9
 */
final class ReDosageTest {

    @Test
    void delegatesItemsToOrigin() {
        MatcherAssert.assertThat(
            "does not delegate items to origin dosage",
            new ReDosage(new Dosage.Empty()).items(),
            Matchers.empty()
        );
    }

    @Test
    void delegatesHasNextToOrigin() {
        MatcherAssert.assertThat(
            "does not delegate hasNext to origin dosage",
            new ReDosage(new Dosage.Empty()).hasNext(),
            Matchers.is(false)
        );
    }

    @Test
    void wrapsNextResultInReDosage() {
        MatcherAssert.assertThat(
            "does not wrap next result in ReDosage",
            new ReDosage(
                new Dosage() {
                    @Override
                    public List<Map<String, AttributeValue>> items() {
                        return Collections.emptyList();
                    }

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public Dosage next() {
                        return new Dosage.Empty();
                    }
                }
            ).next(),
            Matchers.instanceOf(ReDosage.class)
        );
    }
}
