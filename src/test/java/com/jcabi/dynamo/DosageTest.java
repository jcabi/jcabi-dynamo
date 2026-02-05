/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Dosage.Empty}.
 * @since 0.1
 */
final class DosageTest {

    @Test
    void returnsEmptyItemsList() {
        MatcherAssert.assertThat(
            "does not return empty items list",
            new Dosage.Empty().items(),
            Matchers.empty()
        );
    }

    @Test
    void returnsFalseForHasNext() {
        MatcherAssert.assertThat(
            "does not return false for hasNext on empty dosage",
            new Dosage.Empty().hasNext(),
            Matchers.is(false)
        );
    }

    @Test
    void throwsOnNextWhenEmpty() {
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> new Dosage.Empty().next()
        );
    }
}
