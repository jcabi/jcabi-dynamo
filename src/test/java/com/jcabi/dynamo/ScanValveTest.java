/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Test case for {@link ScanValve}.
 * @since 0.1
 */
final class ScanValveTest {

    @Test
    @SuppressWarnings("unchecked")
    void fetchesDataWithNoNext() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            ScanResponse.builder()
                .items(
                    Collections.singletonList(Collections.emptyMap())
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "should not has next",
            new ScanValve().fetch(
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
            ScanResponse.builder()
                .items(
                    Collections.singletonList(item)
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "should has items",
            new ScanValve().fetch(
                credentials, "table",
                new Conditions(), new ArrayList<>(0)
            ).items(),
            Matchers.hasItem(item)
        );
    }

    @Test
    void countsItemsViaScan() {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final int expected = new Random().nextInt(100) + 1;
        Mockito.doReturn(
            ScanResponse.builder()
                .count(expected)
                .consumedCapacity(
                    ConsumedCapacity.builder()
                        .capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not return correct count from scan",
            new ScanValve().count(
                creds, "c\u00f6unt-tbl", new Conditions()
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
                Mockito.doThrow(
                    SdkClientException.create("f\u00e4iled")
                ).when(aws).scan(Mockito.any(ScanRequest.class));
                new ScanValve().fetch(
                    creds, "f\u00e4il-tbl",
                    new Conditions(), new ArrayList<>(0)
                );
            }
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsNextWhenMorePages() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        Mockito.doReturn(
            ScanResponse.builder()
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
                    ConsumedCapacity.builder()
                        .capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not report next when more pages exist",
            new ScanValve().fetch(
                creds, "p\u00e4ge-tbl",
                new Conditions(), new ArrayList<>(0)
            ).hasNext(),
            Matchers.is(true)
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesNextPage() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final Map<String, AttributeValue> item =
            Collections.singletonMap(
                "s\u00f6rt",
                AttributeValue.builder().s("v\u00e4l2").build()
            );
        Mockito.doReturn(
            ScanResponse.builder()
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
                    ConsumedCapacity.builder()
                        .capacityUnits(1d).build()
                )
                .build()
        ).doReturn(
            ScanResponse.builder()
                .items(Collections.singletonList(item))
                .consumedCapacity(
                    ConsumedCapacity.builder()
                        .capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not return correct items on next page",
            new ScanValve().fetch(
                creds, "n\u00e9xt-tbl",
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
                    ScanResponse.builder()
                        .items(
                            Collections.singletonList(
                                Collections.emptyMap()
                            )
                        )
                        .consumedCapacity(
                            ConsumedCapacity.builder()
                                .capacityUnits(1d).build()
                        )
                        .build()
                ).when(aws).scan(Mockito.any(ScanRequest.class));
                new ScanValve().fetch(
                    creds, "n\u00f6-next-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).next();
            }
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchesWithCustomLimit() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        Mockito.doReturn(
            ScanResponse.builder()
                .items(
                    Collections.singletonList(Collections.emptyMap())
                )
                .consumedCapacity(
                    ConsumedCapacity.builder()
                        .capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with custom limit",
            new ScanValve()
                .withLimit(5)
                .fetch(
                    creds, "l\u00efmit-tbl",
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
            ScanResponse.builder()
                .items(
                    Collections.singletonList(Collections.emptyMap())
                )
                .consumedCapacity(
                    ConsumedCapacity.builder()
                        .capacityUnits(1d).build()
                )
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with attribute to get",
            new ScanValve()
                .withAttributeToGet("\u00e4ttr")
                .fetch(
                    creds, "\u00e4ttr-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

}
