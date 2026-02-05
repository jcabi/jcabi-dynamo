/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;

/**
 * Test case for {@link PrintableConsumedCapacity}.
 * @since 0.22
 */
final class PrintableConsumedCapacityTest {

    @Test
    void printsEmptyStringForNullCapacity() {
        MatcherAssert.assertThat(
            "does not print empty string for null capacity",
            new PrintableConsumedCapacity(null).print(),
            Matchers.equalTo("")
        );
    }

    @Test
    void printsFormattedCapacityUnits() {
        MatcherAssert.assertThat(
            "does not print formatted capacity units",
            new PrintableConsumedCapacity(
                ConsumedCapacity.builder()
                    .capacityUnits(12.5)
                    .tableName("t\u00e9st")
                    .build()
            ).print(),
            Matchers.equalTo("12.50 units")
        );
    }
}
