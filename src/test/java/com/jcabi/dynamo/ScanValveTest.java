/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
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
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

/**
 * Test case for {@link ScanValve}.
 *
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
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(ScanValveTest.capacity())
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
                .items(Collections.singletonList(item))
                .consumedCapacity(ScanValveTest.capacity())
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
                .count(expected).consumedCapacity(ScanValveTest.capacity())
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not return correct count from scan",
            new ScanValve().count(
                creds, "cöunt-tbl", new Conditions()
            ),
            Matchers.equalTo(expected)
        );
    }

    @Test
    void countsEveryPageIncludingAnEmptyPage() {
        final Credentials credentials = Mockito.mock(Credentials.class);
        Mockito.doReturn(ScanValveTest.threePages()).when(credentials).aws();
        MatcherAssert.assertThat(
            "must not lose items from later pages",
            new ScanValve().count(credentials, "pages", new Conditions()),
            Matchers.equalTo(5)
        );
    }

    @Test
    void passesContinuationKeyToEveryNextScan() {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = ScanValveTest.threePages();
        Mockito.doReturn(aws).when(credentials).aws();
        final Conditions conditions = new Conditions().with("hash", "value");
        new ScanValve().count(credentials, "pages", conditions);
        final ArgumentCaptor<ScanRequest> requests =
            ArgumentCaptor.forClass(ScanRequest.class);
        Mockito.verify(aws, Mockito.times(3)).scan(requests.capture());
        final ScanRequest expected = ScanRequest.builder().tableName("pages")
            .scanFilter(conditions).select(Select.COUNT).limit(Integer.MAX_VALUE)
            .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();
        MatcherAssert.assertThat(
            "must not lose options or continuation keys between scans",
            requests.getAllValues(),
            Matchers.equalTo(
                Arrays.asList(
                    expected,
                    expected.toBuilder()
                        .exclusiveStartKey(ScanValveTest.key("first")).build(),
                    expected.toBuilder()
                        .exclusiveStartKey(ScanValveTest.key("second")).build()
                )
            )
        );
    }

    @Test
    void capsCountsLargerThanIntegerMaxValue() {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            ScanResponse.builder().count(Integer.MAX_VALUE - 2).lastEvaluatedKey(
                Collections.singletonMap(
                    "id", AttributeValue.builder().s("last").build()
                )
            ).consumedCapacity(ScanValveTest.capacity()).build()
        ).doReturn(
            ScanResponse.builder().count(4)
                .consumedCapacity(ScanValveTest.capacity()).build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "must not overflow the Collection.size result",
            new ScanValve().count(credentials, "large", new Conditions()),
            Matchers.equalTo(Integer.MAX_VALUE)
        );
    }

    @Test
    void closesClientWhenCountingALaterPageFails() {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            ScanResponse.builder().count(2).lastEvaluatedKey(
                Collections.singletonMap(
                    "id", AttributeValue.builder().s("last").build()
                )
            ).consumedCapacity(ScanValveTest.capacity()).build()
        ).doThrow(SdkClientException.create("second page failed"))
            .when(aws).scan(Mockito.any(ScanRequest.class));
        Assertions.assertThrows(
            SdkClientException.class,
            () -> new ScanValve().count(credentials, "pages", new Conditions())
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
                Mockito.doThrow(
                    SdkClientException.create("fäiled")
                ).when(aws).scan(Mockito.any(ScanRequest.class));
                new ScanValve().fetch(
                    creds, "fäil-tbl",
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
            ScanResponse.builder().items(
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
            ).consumedCapacity(ScanValveTest.capacity())
            .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not report next when more pages exist",
            new ScanValve().fetch(
                creds, "päge-tbl",
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
                "sört",
                AttributeValue.builder().s("väl2").build()
            );
        Mockito.doReturn(
            ScanResponse.builder().items(
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
            ).consumedCapacity(ScanValveTest.capacity())
            .build()
        ).doReturn(
            ScanResponse.builder()
                .items(Collections.singletonList(item))
                .consumedCapacity(ScanValveTest.capacity())
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not return correct items on next page",
            new ScanValve().fetch(
                creds, "néxt-tbl",
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
                    ScanResponse.builder().items(
                        Collections.singletonList(
                            Collections.emptyMap()
                        )
                    ).consumedCapacity(ScanValveTest.capacity())
                    .build()
                ).when(aws).scan(Mockito.any(ScanRequest.class));
                new ScanValve().fetch(
                    creds, "nö-next-tbl",
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
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(ScanValveTest.capacity())
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with custom limit",
            new ScanValve()
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
            ScanResponse.builder()
                .items(Collections.singletonList(Collections.emptyMap()))
                .consumedCapacity(ScanValveTest.capacity())
                .build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        MatcherAssert.assertThat(
            "did not fetch with attribute to get",
            new ScanValve()
                .withAttributeToGet("ättr").fetch(
                    creds, "ättr-tbl",
                    new Conditions(), new ArrayList<>(0)
                ).items(),
            Matchers.hasSize(1)
        );
    }

    private static DynamoDbClient threePages() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(
            ScanResponse.builder().count(2)
                .lastEvaluatedKey(ScanValveTest.key("first"))
                .consumedCapacity(ScanValveTest.capacity()).build()
        ).doReturn(
            ScanResponse.builder().count(0)
                .lastEvaluatedKey(ScanValveTest.key("second"))
                .consumedCapacity(ScanValveTest.capacity()).build()
        ).doReturn(
            ScanResponse.builder().count(3)
                .consumedCapacity(ScanValveTest.capacity()).build()
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        return aws;
    }

    private static Map<String, AttributeValue> key(final String value) {
        return Collections.singletonMap(
            "id", AttributeValue.builder().s(value).build()
        );
    }

    private static ConsumedCapacity capacity() {
        return ConsumedCapacity.builder().capacityUnits(1d).build();
    }
}
