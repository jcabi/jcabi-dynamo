/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Test case for {@link AwsTable}.
 * @since 0.1
 */
final class AwsTableTest {

    /**
     * Constant for 'key' attribute.
     */
    private static final String KEY = "key";

    @Test
    void savesItemToDynamo() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            PutItemResponse.builder().consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0d).build()
            ).build()
        ).when(aws).putItem(Mockito.any(PutItemRequest.class));
        Mockito.doReturn(
            DescribeTableResponse.builder().table(
                TableDescription.builder().keySchema(
                    KeySchemaElement.builder().attributeName(AwsTableTest.KEY).build()
                ).build()
            ).build()
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final Table table = new AwsTable(
            credentials, Mockito.mock(Region.class), "table-name"
        );
        table.put(
            new Attributes().with(
                "attribute-1",
                AttributeValue.builder().s("value-1").build()
            )
        );
        Mockito.verify(aws).putItem(Mockito.any(PutItemRequest.class));
    }

    @Test
    void deletesItemFromDynamo() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            DeleteItemResponse.builder().consumedCapacity(
                ConsumedCapacity.builder().capacityUnits(1.0d).build()
            ).build()
        ).when(aws).deleteItem(Mockito.any(DeleteItemRequest.class));
        Mockito.doReturn(
            DescribeTableResponse.builder().table(
                TableDescription.builder().keySchema(
                    KeySchemaElement.builder().attributeName(AwsTableTest.KEY).build()
                ).build()
            ).build()
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final Table table = new AwsTable(
            credentials, Mockito.mock(Region.class), "table-name-2"
        );
        table.delete(
            new Attributes().with(
                "attribute-2",
                AttributeValue.builder().s("value-2").build()
            )
        );
        Mockito.verify(aws).deleteItem(Mockito.any(DeleteItemRequest.class));
    }

    @Test
    void returnsKeysFromDescribeTable() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            DescribeTableResponse.builder().table(
                TableDescription.builder().keySchema(
                    KeySchemaElement.builder().attributeName(AwsTableTest.KEY).build()
                ).build()
            ).build()
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final AwsTable table = new AwsTable(
            credentials, Mockito.mock(Region.class), "table-with-key"
        );
        MatcherAssert.assertThat(
            "keys() must return the single primary key declared in the table schema",
            table.keys(),
            Matchers.contains(AwsTableTest.KEY)
        );
    }

    @Test
    void returnsCompositeKeysInDeclaredOrder() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            DescribeTableResponse.builder().table(
                TableDescription.builder().keySchema(
                    KeySchemaElement.builder().attributeName("hash-key").build(),
                    KeySchemaElement.builder().attributeName("range-key").build()
                ).build()
            ).build()
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final AwsTable table = new AwsTable(
            credentials, Mockito.mock(Region.class), "composite-table"
        );
        MatcherAssert.assertThat(
            "keys() must preserve the order of the composite primary key",
            table.keys(),
            Matchers.contains("hash-key", "range-key")
        );
    }

    @Test
    void requestsDescribeForOwnTable() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            DescribeTableResponse.builder().table(
                TableDescription.builder().keySchema(
                    KeySchemaElement.builder().attributeName(AwsTableTest.KEY).build()
                ).build()
            ).build()
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final String name = "my-described-table";
        final AwsTable table = new AwsTable(
            credentials, Mockito.mock(Region.class), name
        );
        table.keys();
        Mockito.verify(aws).describeTable(
            DescribeTableRequest.builder().tableName(name).build()
        );
    }
}
