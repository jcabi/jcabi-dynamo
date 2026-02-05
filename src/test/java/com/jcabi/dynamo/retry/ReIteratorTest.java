/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link ReIterator}.
 * @since 0.9
 */
final class ReIteratorTest {

    @Test
    void delegatesHasNextToOrigin() {
        MatcherAssert.assertThat(
            "does not delegate hasNext to origin iterator",
            new ReIterator<>(
                Arrays.asList("f\u00f6o", "b\u00e4r").iterator()
            ).hasNext(),
            Matchers.is(true)
        );
    }

    @Test
    void delegatesNextToOrigin() {
        MatcherAssert.assertThat(
            "does not delegate next to origin iterator",
            new ReIterator<>(
                Arrays.asList("f\u00f6o", "b\u00e4r").iterator()
            ).next(),
            Matchers.equalTo("f\u00f6o")
        );
    }

    @Test
    void delegatesRemoveToOrigin() {
        final List<String> items = new ArrayList<>(
            Arrays.asList("\u00fcbung")
        );
        final Iterator<String> iter = items.iterator();
        iter.next();
        new ReIterator<>(iter).remove();
        MatcherAssert.assertThat(
            "does not delegate remove to origin iterator",
            items,
            Matchers.empty()
        );
    }

    @Test
    void returnsFalseWhenOriginIsExhausted() {
        final List<String> items = Arrays.asList("\u00f6nly");
        final Iterator<String> iter = items.iterator();
        iter.next();
        MatcherAssert.assertThat(
            "does not return false when origin is exhausted",
            new ReIterator<>(iter).hasNext(),
            Matchers.is(false)
        );
    }
}
