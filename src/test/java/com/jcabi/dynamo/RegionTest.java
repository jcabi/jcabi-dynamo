/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.util.UUID;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Test case for {@link Region}.
 * @since 0.1
 */
final class RegionTest {

    @Test
    void appendsPrefixesToTableNames() {
        final Table table = Mockito.mock(Table.class);
        final Region region = Mockito.mock(Region.class);
        Mockito.doReturn(table).when(region).table(Mockito.anyString());
        new Region.Prefixed(region, "foo-").table("test");
        Mockito.verify(region).table("foo-test");
    }

    @Test
    void delegatesAwsToCredentials() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        final Credentials creds = Mockito.mock(Credentials.class);
        Mockito.doReturn(aws).when(creds).aws();
        MatcherAssert.assertThat(
            "did not delegate aws() to credentials",
            new Region.Simple(creds).aws(),
            Matchers.is(aws)
        );
    }

    @Test
    void delegatesPrefixedAwsToOrigin() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        final Region origin = Mockito.mock(Region.class);
        Mockito.doReturn(aws).when(origin).aws();
        MatcherAssert.assertThat(
            "did not delegate prefixed aws() to origin",
            new Region.Prefixed(origin, "prfx-").aws(),
            Matchers.is(aws)
        );
    }

    @Test
    void appendsNonAsciiPrefix() {
        final Table table = Mockito.mock(Table.class);
        final Region origin = Mockito.mock(Region.class);
        Mockito.doReturn(table).when(origin).table(Mockito.anyString());
        final String name = UUID.randomUUID().toString();
        new Region.Prefixed(origin, "prëfïx-").table(name);
        Mockito.verify(origin).table(
            String.format("prëfïx-%s", name)
        );
    }

    @Test
    void appendsEmptyPrefix() {
        final Table table = Mockito.mock(Table.class);
        final Region origin = Mockito.mock(Region.class);
        Mockito.doReturn(table).when(origin).table(Mockito.anyString());
        final String name = UUID.randomUUID().toString();
        new Region.Prefixed(origin, "").table(name);
        Mockito.verify(origin).table(name);
    }

}
