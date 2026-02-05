/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link Credentials}.
 * @since 0.1
 */
final class CredentialsTest {

    @Test
    void instantiatesAwsClient() {
        MatcherAssert.assertThat(
            "not null",
            new Credentials.Simple(
                "ABABABABABABABABABEF",
                "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCEF"
            ).aws(),
            Matchers.notNullValue()
        );
    }

    @Test
    void instantiatesAwsClientWithCustomRegion() {
        MatcherAssert.assertThat(
            "not null",
            new Credentials.Simple(
                "ABABABABABABABABABAB",
                "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE",
                "eu-west-1"
            ).aws(),
            Matchers.notNullValue()
        );
    }

}
