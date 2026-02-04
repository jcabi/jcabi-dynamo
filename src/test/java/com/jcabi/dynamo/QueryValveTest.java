/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * Test case for {@link QueryValve}.
 * @since 0.1
 */
final class QueryValveTest {

    @Test
    @SuppressWarnings("unchecked")
    void fetchesData() throws Exception {
        final Valve valve = new QueryValve();
        final Credentials credentials = Mockito.mock(Credentials.class);
        final ImmutableMap<String, AttributeValue> item =
            new ImmutableMap.Builder<String, AttributeValue>()
                .build();
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            QueryResponse.builder()
                .items(
                    Collections.singletonList(item)
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0d).build()
                )
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        final Dosage dosage = valve.fetch(
            credentials, "table",
            new Conditions(), new ArrayList<>(0)
        );
        MatcherAssert.assertThat("should be false", dosage.hasNext(), Matchers.is(false));
        MatcherAssert.assertThat("should has item", dosage.items(), Matchers.hasItem(item));
    }

}
