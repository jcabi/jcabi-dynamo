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
                "tést-tbl",
                new Attributes().with(
                    "kéy",
                    AttributeValue.builder().s("vál").build()
                ),
                new Array<>("kéy")
            ).has("kéy"),
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
                "tést-tbl",
                new Attributes().with(
                    "näme",
                    AttributeValue.builder().s("däta").build()
                ),
                new Array<>("näme")
            ).get("näme"),
            Matchers.equalTo(
                AttributeValue.builder().s("däta").build()
            )
        );
    }

    @Test
    void fetchesAttributeFromDynamo() throws IOException {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final String attr = "ättr";
        Mockito.doReturn(
            GetItemResponse.builder().item(
                Collections.singletonMap(
                    attr,
                    AttributeValue.builder().s("résult").build()
                )
            ).consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0).build()
            )
            .build()
        ).when(aws).getItem(Mockito.any(GetItemRequest.class));
        MatcherAssert.assertThat(
            "did not fetch attribute from DynamoDB",
            new AwsItem(
                creds, Mockito.mock(AwsFrame.class), "fétch-tbl",
                new Attributes().with(
                    "pk",
                    AttributeValue.builder().s("fétch-pk").build()
                ),
                new Array<>("pk")
            ).get(attr),
            Matchers.equalTo(
                AttributeValue.builder().s("résult").build()
            )
        );
    }

    @Test
    void checksExistenceViaDynamo() throws IOException {
        final Credentials creds = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(creds).aws();
        final String attr = "chéck";
        Mockito.doReturn(
            GetItemResponse.builder().item(
                Collections.singletonMap(
                    attr,
                    AttributeValue.builder().s("présent").build()
                )
            ).consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0).build()
            )
            .build()
        ).when(aws).getItem(Mockito.any(GetItemRequest.class));
        MatcherAssert.assertThat(
            "did not detect attribute existence via DynamoDB",
            new AwsItem(
                creds, Mockito.mock(AwsFrame.class), "häs-tbl",
                new Attributes().with(
                    "pk",
                    AttributeValue.builder().s("häs-pk").build()
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
        final String attr = "üpd";
        Mockito.doReturn(
            UpdateItemResponse.builder().attributes(
                Collections.singletonMap(
                    attr,
                    AttributeValue.builder().s("néw").build()
                )
            ).consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0).build()
            )
            .build()
        ).when(aws).updateItem(Mockito.any(UpdateItemRequest.class));
        MatcherAssert.assertThat(
            "did not return updated attributes",
            new AwsItem(
                creds, Mockito.mock(AwsFrame.class), "üpd-tbl",
                new Attributes().with(
                    "pk",
                    AttributeValue.builder().s("üpd-pk").build()
                ),
                new Array<>("pk")
            ).put(
                attr,
                AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s("néw").build())
                    .action(AttributeAction.PUT)
                    .build()
            ),
            Matchers.hasEntry(
                Matchers.equalTo(attr),
                Matchers.equalTo(
                    AttributeValue.builder().s("néw").build()
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
            Mockito.mock(AwsFrame.class), "absént-tbl",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("absént-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            NoSuchElementException.class,
            () -> item.get("mïssing")
        );
    }

    @Test
    void wrapsExceptionOnGet() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doThrow(SdkClientException.create("böom"))
            .when(aws).getItem(Mockito.any(GetItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "gét-err",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("gét-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            IOException.class,
            () -> item.get("fäil")
        );
    }

    @Test
    void wrapsExceptionOnHas() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doThrow(SdkClientException.create("böom"))
            .when(aws).getItem(Mockito.any(GetItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "häs-err",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("häs-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            IOException.class,
            () -> item.has("fäil")
        );
    }

    @Test
    void wrapsExceptionOnPut() {
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doThrow(SdkClientException.create("böom"))
            .when(aws).updateItem(Mockito.any(UpdateItemRequest.class));
        final Item item = new AwsItem(
            AwsItemTest.mocked(aws),
            Mockito.mock(AwsFrame.class), "püt-err",
            new Attributes().with(
                "pk",
                AttributeValue.builder().s("püt-pk").build()
            ),
            new Array<>("pk")
        );
        Assertions.assertThrows(
            IOException.class,
            () -> item.put(
                new AttributeUpdates().with(
                    "fäil",
                    AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s("vál").build())
                        .action(AttributeAction.PUT)
                        .build()
                )
            )
        );
    }

    @Test
    void distinguishesDifferentItems() {
        final Credentials creds = new Credentials.Simple(
            "kéy1", "sécret1"
        );
        final AwsTable first = new AwsTable(
            creds, new Region.Simple(creds), "täble-one"
        );
        final AwsTable second = new AwsTable(
            creds, new Region.Simple(creds), "täble-two"
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
                "främe-tbl",
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
            .item(Collections.emptyMap()).consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0).build()
            )
            .build();
    }
}
