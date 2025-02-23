/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;

/**
 * Test case for {@link AwsTable}.
 * @since 0.1
 */
final class AwsTableTest {

    /**
     * Constant for 'tableName' attribute.
     */
    private static final String TABLE_NAME = "tableName";

    /**
     * Constant for 'key' attribute.
     */
    private static final String KEY = "key";

    @Test
    void savesItemToDynamo() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final AmazonDynamoDB aws = Mockito.mock(AmazonDynamoDB.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            new PutItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(1.0d)
            )
        ).when(aws).putItem(Mockito.any(PutItemRequest.class));
        Mockito.doReturn(
            new DescribeTableResult().withTable(
                new TableDescription().withKeySchema(
                    new KeySchemaElement().withAttributeName(AwsTableTest.KEY)
                )
            )
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final String attr = "attribute-1";
        final AttributeValue value = new AttributeValue("value-1");
        final String name = "table-name";
        final Table table = new AwsTable(
            credentials, Mockito.mock(Region.class), name
        );
        table.put(new Attributes().with(attr, value));
        Mockito.verify(aws).putItem(
            (PutItemRequest) MockitoHamcrest.argThat(
                Matchers.allOf(
                    Matchers.hasProperty(
                        AwsTableTest.TABLE_NAME,
                        Matchers.equalTo(name)
                    ),
                    Matchers.hasProperty(
                        "item",
                        Matchers.hasEntry(
                            Matchers.equalTo(attr),
                            Matchers.equalTo(value)
                        )
                    )
                )
            )
        );
    }

    @Test
    void deletesItemFromDynamo() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final AmazonDynamoDB aws = Mockito.mock(AmazonDynamoDB.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            new DeleteItemResult().withConsumedCapacity(
                new ConsumedCapacity().withCapacityUnits(1.0d)
            )
        ).when(aws).deleteItem(Mockito.any(DeleteItemRequest.class));
        Mockito.doReturn(
            new DescribeTableResult().withTable(
                new TableDescription().withKeySchema(
                    new KeySchemaElement().withAttributeName(AwsTableTest.KEY)
                )
            )
        ).when(aws).describeTable(Mockito.any(DescribeTableRequest.class));
        final String attr = "attribute-2";
        final AttributeValue value = new AttributeValue("value-2");
        final String name = "table-name-2";
        final Table table = new AwsTable(
            credentials, Mockito.mock(Region.class), name
        );
        table.delete(new Attributes().with(attr, value));
        Mockito.verify(aws).deleteItem(
            (DeleteItemRequest) MockitoHamcrest.argThat(
                Matchers.allOf(
                    Matchers.hasProperty(
                        AwsTableTest.TABLE_NAME,
                        Matchers.equalTo(name)
                    ),
                    Matchers.hasProperty(
                        AwsTableTest.KEY,
                        Matchers.hasEntry(
                            Matchers.equalTo(attr),
                            Matchers.equalTo(value)
                        )
                    )
                )
            )
        );
    }
}
