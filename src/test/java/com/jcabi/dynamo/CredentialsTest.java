/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Test case for {@link Credentials}.
 * @since 0.1
 */
final class CredentialsTest {

    @Test
    void instantiatesAwsClient() {
        final DynamoDbClient aws = new Credentials.Simple(
            "ABABABABABABABABABEF",
            "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCEF"
        ).aws();
        MatcherAssert.assertThat(
            "Simple credentials did not create AWS client",
            aws,
            Matchers.notNullValue()
        );
        aws.close();
    }

    @Test
    void instantiatesAwsClientWithCustomRegion() {
        final DynamoDbClient aws = new Credentials.Simple(
            "ABABABABABABABABABAB",
            "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE",
            "eu-west-1"
        ).aws();
        MatcherAssert.assertThat(
            "Simple credentials with region did not create AWS client",
            aws,
            Matchers.notNullValue()
        );
        aws.close();
    }

    @Test
    void formatsSimpleAsString() {
        MatcherAssert.assertThat(
            "Simple credentials did not format as region/key",
            new Credentials.Simple(
                "t\u00e9st-k\u00e9y-000000000",
                "s\u00e9cr\u00e9t-0000000000000000000000000000",
                "us-\u00e9ast-1"
            ).toString(),
            Matchers.equalTo("us-\u00e9ast-1/t\u00e9st-k\u00e9y-000000000")
        );
    }

    @Test
    void instantiatesAssumedClient() {
        final DynamoDbClient aws = new Credentials.Assumed().aws();
        MatcherAssert.assertThat(
            "Assumed credentials did not create AWS client",
            aws,
            Matchers.notNullValue()
        );
        aws.close();
    }

    @Test
    void instantiatesAssumedClientWithRegion() {
        final DynamoDbClient aws =
            new Credentials.Assumed("eu-west-1").aws();
        MatcherAssert.assertThat(
            "Assumed credentials with region did not create AWS client",
            aws,
            Matchers.notNullValue()
        );
        aws.close();
    }

    @Test
    void formatsAssumedAsString() {
        MatcherAssert.assertThat(
            "Assumed credentials did not format as region",
            new Credentials.Assumed("us-w\u00e9st-2").toString(),
            Matchers.equalTo("us-w\u00e9st-2")
        );
    }

    @Test
    void instantiatesDirectClient() {
        final DynamoDbClient aws = new Credentials.Direct(
            new Credentials.Simple(
                "ABABABABABABABABAB00",
                "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABC00"
            ),
            "http://localhost:8080"
        ).aws();
        MatcherAssert.assertThat(
            "Direct credentials did not create AWS client",
            aws,
            Matchers.notNullValue()
        );
        aws.close();
    }

    @Test
    void instantiatesDirectClientWithPort() {
        final DynamoDbClient aws = new Credentials.Direct(
            new Credentials.Simple(
                "ABABABABABABABABAB01",
                "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABC01"
            ),
            12_345
        ).aws();
        MatcherAssert.assertThat(
            "Direct credentials with port did not create AWS client",
            aws,
            Matchers.notNullValue()
        );
        aws.close();
    }

    @Test
    void formatsDirectAsString() {
        MatcherAssert.assertThat(
            "Direct credentials did not format with origin and endpoint",
            new Credentials.Direct(
                new Credentials.Simple(
                    "ABABABABABABABABAB02",
                    "ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABC02",
                    "us-\u00e9ast-1"
                ),
                "http://loc\u00e4l:9999"
            ).toString(),
            Matchers.allOf(
                Matchers.containsString("us-\u00e9ast-1"),
                Matchers.containsString("http://loc\u00e4l:9999")
            )
        );
    }

}
