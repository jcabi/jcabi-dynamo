/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
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
        final Credentials creds = new Credentials.Simple(
            "ABABABABABABABABABEF",
            "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCEF"
        );
        MatcherAssert.assertThat("not null", creds.aws(), Matchers.notNullValue());
    }

    @Test
    void instantiatesAwsClientWithCustomRegion() {
        final Credentials creds = new Credentials.Simple(
            "ABABABABABABABABABAB",
            "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE",
            "eu-west-1"
        );
        MatcherAssert.assertThat("not null", creds.aws(), Matchers.notNullValue());
    }

}
