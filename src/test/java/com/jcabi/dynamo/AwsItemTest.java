/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.immutable.Array;
import java.io.IOException;
import java.util.Collections;
import java.util.NoSuchElementException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Test case for {@link AwsItem}.
 * @since 0.21
 */
@SuppressWarnings("PMD.TooManyMethods")
final class AwsItemTest {

    @Test
    void comparesToItself() {
        final Credentials creds = new Credentials.Simple("key", "secret");
        final AwsTable table = new AwsTable(
            creds, new Region.Simple(creds), "table-name"
        );
        final AwsFrame frame = new AwsFrame(creds, table, table.name());
        MatcherAssert.assertThat(
            "should equal to itself",
            new AwsItem(
                creds, frame, table.name(),
                new Attributes(), new Array<>()
            ),
            Matchers.equalTo(
                new AwsItem(
                    creds, frame, table.name(),
                    new Attributes(), new Array<>()
                )
            )
        );
    }

    @Test
    void checksPreloadedAttribute() throws IOException {
        MatcherAssert.assertThat(
            "did not find pre-loaded attribute",
            new AwsItem(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsFrame.class),
                "t\u00e9st-tbl",
                new Attributes().with(
                    "k\u00e9y",
                    AttributeValue.builder().s("v\u00e1l").build()
                ),
                new Array<>("k\u00e9y")
            ).has("k\u00e9y"),
            Matchers.is(true)
        );
    }

    @Test
    void returnsPreloadedAttribute() throws IOException {
        MatcherAssert.assertThat(
            "did not return pre-loaded attribute value",
            new AwsItem(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsFrame.class),
                "t\u00e9st-tbl",
                new Attributes().with(
                    "n\u00e4me",
                    AttributeValue.builder().s("d\u00e4ta").build()
                ),
                new Array<>("n\u00e4me")
            ).get("n\u00e4me"),
            Matchers.equalTo(
                AttributeValue.builder().s("d\u00e4ta").build()
            )
        );
    }

    @Test
    void fetchesAttributeFromDynamo() throws IOException {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final String attr = "\u00e4ttr";
        Mockito.doReturn(
            GetItemResponse.builder()
                .item(
                    Collections.singletonMap(
                        attr,
                        AttributeValue.builder().s("r\u00e9sult").build()
                    )
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0).build()
                )
                .build()
        ).when(aws).getItem(Mockito.any(GetItemRequest.class));
        MatcherAssert.assertThat(
            "did not fetch attribute from DynamoDB",
            new AwsItem(
                creds, Mockito.mock(AwsFrame.class), "f\u00e9tch-tbl",
                new Attributes().with(
                    "pk",
                    AttributeValue.builder().s("f\u00e9tch-pk").build()
                ),
                new Array<>("pk")
            ).get(attr),
            Matchers.equalTo(
                AttributeValue.builder().s("r\u00e9sult").build()
            )
        );
    }

    @Test
    void checksExistenceViaDynamo() throws IOException {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final String attr = "ch\u00e9ck";
        Mockito.doReturn(
            GetItemResponse.builder()
                .item(
                    Collections.singletonMap(
                        attr,
                        AttributeValue.builder().s("pr\u00e9sent").build()
                    )
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0).build()
                )
                .build()
        ).when(aws).getItem(Mockito.any(GetItemRequest.class));
        MatcherAssert.assertThat(
            "did not detect attribute existence via DynamoDB",
            new AwsItem(
                creds, Mockito.mock(AwsFrame.class), "h\u00e4s-tbl",
                new Attributes().with(
                    "pk",
                    AttributeValue.builder().s("h\u00e4s-pk").build()
                ),
                new Array<>("pk")
            ).has(attr),
            Matchers.is(true)
        );
    }

    @Test
    void updatesAttribute() throws IOException {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final String attr = "\u00fcpd";
        Mockito.doReturn(
            UpdateItemResponse.builder()
                .attributes(
                    Collections.singletonMap(
                        attr,
                        AttributeValue.builder().s("n\u00e9w").build()
                    )
                )
                .consumedCapacity(
                    ConsumedCapacity.builder().capacityUnits(1.0).build()
                )
                .build()
        ).when(aws).updateItem(Mockito.any(UpdateItemRequest.class));
        MatcherAssert.assertThat(
            "did not return updated attributes",
            new AwsItem(
                creds, Mockito.mock(AwsFrame.class), "\u00fcpd-tbl",
                new Attributes().with(
                    "pk",
                    AttributeValue.builder().s("\u00fcpd-pk").build()
                ),
                new Array<>("pk")
            ).put(
                attr,
                AttributeValueUpdate.builder()
                    .value(
                        AttributeValue.builder().s("n\u00e9w").build()
                    )
                    .action(AttributeAction.PUT)
                    .build()
            ),
            Matchers.hasEntry(
                Matchers.equalTo(attr),
                Matchers.equalTo(
                    AttributeValue.builder().s("n\u00e9w").build()
                )
            )
        );
    }

    @Test
    void throwsOnAbsentAttribute() throws IOException {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(AwsItemTest.empty())
            .when(aws).getItem(Mockito.any(GetItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "abs\u00e9nt-tbl",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("abs\u00e9nt-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            NoSuchElementException.class,
            () -> item.get("m\u00efssing")
        );
    }

    @Test
    void wrapsExceptionOnGet() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doThrow(SdkClientException.create("b\u00f6om"))
            .when(aws).getItem(Mockito.any(GetItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "g\u00e9t-err",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("g\u00e9t-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            IOException.class,
            () -> item.get("f\u00e4il")
        );
    }

    @Test
    void wrapsExceptionOnHas() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doThrow(SdkClientException.create("b\u00f6om"))
            .when(aws).getItem(Mockito.any(GetItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "h\u00e4s-err",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("h\u00e4s-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            IOException.class,
            () -> item.has("f\u00e4il")
        );
    }

    @Test
    void wrapsExceptionOnPut() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doThrow(SdkClientException.create("b\u00f6om"))
            .when(aws).updateItem(Mockito.any(UpdateItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "p\u00fct-err",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("p\u00fct-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            IOException.class,
            () -> item.put(
                new AttributeUpdates().with(
                    "f\u00e4il",
                    AttributeValueUpdate.builder()
                        .value(
                            AttributeValue.builder().s("v\u00e1l").build()
                        )
                        .action(AttributeAction.PUT)
                        .build()
                )
            )
        );
    }

    @Test
    void distinguishesDifferentItems() {
        final Credentials creds = new Credentials.Simple(
            "k\u00e9y1", "s\u00e9cret1"
        );
        final AwsTable first = new AwsTable(
            creds, new Region.Simple(creds), "t\u00e4ble-one"
        );
        final AwsTable second = new AwsTable(
            creds, new Region.Simple(creds), "t\u00e4ble-two"
        );
        MatcherAssert.assertThat(
            "items with different tables should not be equal",
            new AwsItem(
                creds,
                new AwsFrame(creds, first, first.name()),
                first.name(),
                new Attributes(),
                new Array<>()
            ),
            Matchers.not(
                Matchers.equalTo(
                    new AwsItem(
                        creds,
                        new AwsFrame(creds, second, second.name()),
                        second.name(),
                        new Attributes(),
                        new Array<>()
                    )
                )
            )
        );
    }

    @Test
    void returnsFrame() {
        MatcherAssert.assertThat(
            "did not return frame instance",
            new AwsItem(
                Mockito.mock(Credentials.class),
                Mockito.mock(AwsFrame.class),
                "fr\u00e4me-tbl",
                new Attributes(),
                new Array<>()
            ).frame(),
            Matchers.instanceOf(Frame.class)
        );
    }

    /**
     * Creates a Credentials mock that returns the given DynamoDbClient.
     * @param aws DynamoDbClient mock
     * @return Credentials mock
     */
    private static Credentials mocked(final DynamoDbClient aws) {
        final Credentials creds = Mockito.mock(Credentials.class);
        Mockito.doReturn(aws).when(creds).aws();
        return creds;
    }

    /**
     * Creates an empty GetItemResponse with consumed capacity.
     * @return GetItemResponse with no items
     */
    private static GetItemResponse empty() {
        return GetItemResponse.builder()
            .item(Collections.emptyMap())
            .consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0).build()
            )
            .build();
    }

}
