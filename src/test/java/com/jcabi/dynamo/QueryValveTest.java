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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.Select;

/**
 * Test case for {@link QueryValve}.
 *
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
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(QueryValveTest.capacity())
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
                .items(Collections.singletonList(item))
                .consumedCapacity(QueryValveTest.capacity())
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
                .count(expected).consumedCapacity(QueryValveTest.capacity())
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not return wrong count",
            new QueryValve().count(
                credentials, "cöunt-tbl", new Conditions()
            ),
            Matchers.equalTo(expected)
        );
    }

    @Test
    void countsEveryPageIncludingAnEmptyPage() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        final Map<String, AttributeValue> first = Collections.singletonMap(
            "id", AttributeValue.builder().s("first").build()
        );
        final Map<String, AttributeValue> second = Collections.singletonMap(
            "id", AttributeValue.builder().s("second").build()
        );
        Mockito.doReturn(
            QueryResponse.builder().count(2).lastEvaluatedKey(first)
                .consumedCapacity(QueryValveTest.capacity()).build()
        ).doReturn(
            QueryResponse.builder().count(0).lastEvaluatedKey(second)
                .consumedCapacity(QueryValveTest.capacity()).build()
        ).doReturn(
            QueryResponse.builder().count(3)
                .consumedCapacity(QueryValveTest.capacity()).build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        final Conditions conditions = new Conditions().with("hash", "value");
        MatcherAssert.assertThat(
            "must include the page after an empty page",
            new QueryValve().withIndexName("index").withConsistentRead(false)
                .count(credentials, "pages", conditions),
            Matchers.equalTo(5)
        );
        final ArgumentCaptor<QueryRequest> requests =
            ArgumentCaptor.forClass(QueryRequest.class);
        Mockito.verify(aws, Mockito.times(3)).query(requests.capture());
        final QueryRequest expected = QueryRequest.builder().tableName("pages")
            .keyConditions(conditions).consistentRead(false).indexName("index")
            .select(Select.COUNT).limit(Integer.MAX_VALUE)
            .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();
        MatcherAssert.assertThat(
            "must preserve query options and advance each continuation key",
            requests.getAllValues(),
            Matchers.contains(
                expected, expected.toBuilder().exclusiveStartKey(first).build(),
                expected.toBuilder().exclusiveStartKey(second).build()
            )
        );
        Mockito.verify(aws).close();
    }

    @Test
    void capsCountsLargerThanIntegerMaxValue() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            QueryResponse.builder().count(Integer.MAX_VALUE - 2)
                .lastEvaluatedKey(
                    Collections.singletonMap(
                        "id", AttributeValue.builder().s("last").build()
                    )
                ).consumedCapacity(QueryValveTest.capacity()).build()
        ).doReturn(
            QueryResponse.builder().count(4)
                .consumedCapacity(QueryValveTest.capacity()).build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "must not overflow the Collection.size result",
            new QueryValve().count(credentials, "large", new Conditions()),
            Matchers.equalTo(Integer.MAX_VALUE)
        );
    }

    @Test
    void closesClientWhenCountingALaterPageFails() {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            QueryResponse.builder().count(2).lastEvaluatedKey(
                Collections.singletonMap(
                    "id", AttributeValue.builder().s("last").build()
                )
            ).consumedCapacity(QueryValveTest.capacity()).build()
        ).doThrow(SdkClientException.create("second page failed"))
            .when(aws).query(Mockito.any(QueryRequest.class));
        Assertions.assertThrows(
            IOException.class,
            () -> new QueryValve().count(credentials, "pages", new Conditions())
        );
        Mockito.verify(aws).close();
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
                Mockito.doThrow(SdkClientException.create("fäiled"))
                    .when(aws).query(Mockito.any(QueryRequest.class));
                new QueryValve().fetch(
                    creds, "fäil-tbl",
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
                Mockito.doThrow(SdkClientException.create("fäiled"))
                    .when(aws).query(Mockito.any(QueryRequest.class));
                new QueryValve().count(
                    creds, "cöunt-err", new Conditions()
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
            QueryResponse.builder().items(
                Collections.singletonList(
                    Collections.singletonMap(
                        "häsh",
                        AttributeValue.builder()
                            .s("väl").build()
                    )
                )
            ).lastEvaluatedKey(
                Collections.singletonMap(
                    "häsh",
                    AttributeValue.builder()
                        .s("läst").build()
                )
            ).consumedCapacity(QueryValveTest.capacity())
            .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not report no next when there is next",
            new QueryValve().fetch(
                credentials, "päge-tbl",
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
            "sört",
            AttributeValue.builder().s("väl2").build()
        );
        Mockito.doReturn(
            QueryResponse.builder().items(
                Collections.singletonList(
                    Collections.singletonMap(
                        "sört",
                        AttributeValue.builder()
                            .s("väl1").build()
                    )
                )
            ).lastEvaluatedKey(
                Collections.singletonMap(
                    "sört",
                    AttributeValue.builder()
                        .s("läst").build()
                )
            ).consumedCapacity(QueryValveTest.capacity())
            .build()
        ).doReturn(
            QueryResponse.builder()
                .items(Collections.singletonList(item))
                .consumedCapacity(QueryValveTest.capacity())
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not return wrong items on next page",
            new QueryValve().fetch(
                credentials, "néxt-tbl",
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
                    QueryResponse.builder().items(
                        Collections.singletonList(
                            Collections.emptyMap()
                        )
                    ).consumedCapacity(QueryValveTest.capacity())
                    .build()
                ).when(aws).query(Mockito.any(QueryRequest.class));
                new QueryValve().fetch(
                    creds, "nö-next-tbl",
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
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(QueryValveTest.capacity())
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "should not fail fetching with index name",
            new QueryValve()
                .withIndexName("ïndex").fetch(
                    credentials, "täble",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesWithConsistentRead() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        Mockito.doReturn(
            QueryResponse.builder()
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(QueryValveTest.capacity())
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with consistent read disabled",
            new QueryValve()
                .withConsistentRead(false).fetch(
                    creds, "cönsist-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesWithCustomLimit() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        Mockito.doReturn(
            QueryResponse.builder()
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(QueryValveTest.capacity())
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with custom limit",
            new QueryValve()
                .withLimit(5).fetch(
                    creds, "lïmit-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesWithAttributeToGet() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        Mockito.doReturn(
            QueryResponse.builder()
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(QueryValveTest.capacity())
                .build()
        ).when(aws).query(Mockito.any(QueryRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with attribute to get",
            new QueryValve()
                .withAttributeToGet("ättr").fetch(
                    creds, "ättr-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

    private static ConsumedCapacity capacity() {
        return ConsumedCapacity.builder().capacityUnits(1.0d).build();
    }
}
