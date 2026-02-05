/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
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
    void fetchesDataWithNoNext() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            QueryResponse.builder()
                .items(
                    Collections.singletonList(Collections.emptyMap())
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0d).build()
                )
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should be false",
            new QueryValve().fetch(
                credentials, "table",
                new Conditions(), new ArrayList<>(0)
            ).hasNext(),
            Matchers.is(false)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesDataWithItems() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final Map<String, AttributeValue> item = Collections.emptyMap();
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
        MatcherAssert.assertThat(
            "should has item",
            new QueryValve().fetch(
                credentials, "table",
                new Conditions(), new ArrayList<>(0)
            ).items(),
            Matchers.hasItem(item)
        );
    }

}
