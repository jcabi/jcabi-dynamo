/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
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

    @Test
    void countsItemsViaQuery() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        final int expected = 7;
        Mockito.doReturn(
            QueryResponse.builder()
                .count(expected)
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0d).build()
                )
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not return wrong count",
            new QueryValve().count(
                credentials, "c\u00f6unt-tbl", new Conditions()
            ),
            Matchers.equalTo(expected)
        );
    }

    @Test
    void wrapsExceptionOnFetch() {
        Assertions.assertThrows(
            IOException.class,
            () -> {
                final Credentials creds =
                    Mockito.mock(Credentials.class);
                final DynamoDbClient aws =
                    Mockito.mock(DynamoDbClient.class);
                Mockito.doReturn(aws).when(creds).aws();
                Mockito.doThrow(SdkClientException.create("f\u00e4iled"))
                    .when(aws).query(Mockito.any(QueryRequest.class));
                new QueryValve().fetch(
                    creds, "f\u00e4il-tbl",
                    new Conditions(), new ArrayList<>(0)
                );
            }
        );
    }

    @Test
    void wrapsExceptionOnCount() {
        Assertions.assertThrows(
            IOException.class,
            () -> {
                final Credentials creds =
                    Mockito.mock(Credentials.class);
                final DynamoDbClient aws =
                    Mockito.mock(DynamoDbClient.class);
                Mockito.doReturn(aws).when(creds).aws();
                Mockito.doThrow(SdkClientException.create("f\u00e4iled"))
                    .when(aws).query(Mockito.any(QueryRequest.class));
                new QueryValve().count(
                    creds, "c\u00f6unt-err", new Conditions()
                );
            }
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsNextWhenMorePages() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            QueryResponse.builder()
                .items(
                    Collections.singletonList(
                        Collections.singletonMap(
                            "h\u00e4sh",
                            AttributeValue.builder()
                                .s("v\u00e4l").build()
                        )
                    )
                )
                .lastEvaluatedKey(
                    Collections.singletonMap(
                        "h\u00e4sh",
                        AttributeValue.builder()
                            .s("l\u00e4st").build()
                    )
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0d).build()
                )
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not report no next when there is next",
            new QueryValve().fetch(
                credentials, "p\u00e4ge-tbl",
                new Conditions(), new ArrayList<>(0)
            ).hasNext(),
            Matchers.is(true)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesNextPage() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        final Map<String, AttributeValue> item = Collections.singletonMap(
            "s\u00f6rt",
            AttributeValue.builder().s("v\u00e4l2").build()
        );
        Mockito.doReturn(
            QueryResponse.builder()
                .items(
                    Collections.singletonList(
                        Collections.singletonMap(
                            "s\u00f6rt",
                            AttributeValue.builder()
                                .s("v\u00e4l1").build()
                        )
                    )
                )
                .lastEvaluatedKey(
                    Collections.singletonMap(
                        "s\u00f6rt",
                        AttributeValue.builder()
                            .s("l\u00e4st").build()
                    )
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0d).build()
                )
                .build()
        ).doReturn(
            QueryResponse.builder()
                .items(Collections.singletonList(item))
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0d).build()
                )
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not return wrong items on next page",
            new QueryValve().fetch(
                credentials, "n\u00e9xt-tbl",
                new Conditions(), new ArrayList<>(0)
            ).next().items(),
            Matchers.hasItem(item)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void throwsOnNextWithoutMorePages() {
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> {
                final Credentials creds =
                    Mockito.mock(Credentials.class);
                final DynamoDbClient aws =
                    Mockito.mock(DynamoDbClient.class);
                Mockito.doReturn(aws).when(creds).aws();
                Mockito.doReturn(
                    QueryResponse.builder()
                        .items(
                            Collections.singletonList(
                                Collections.emptyMap()
                            )
                        )
                        .consumedCapacity(
                            ConsumedCapacity.builder()
                                .capacityUnits(1.0d).build()
                        )
                        .build()
                ).when(aws).query(Mockito.any(QueryRequest.class));
                new QueryValve().fetch(
                    creds, "n\u00f6-next-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).next();
            }
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesWithIndexName() throws Exception {
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
            "should not fail fetching with index name",
            new QueryValve()
                .withIndexName("\u00efndex")
                .fetch(
                    credentials, "t\u00e4ble",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

}
