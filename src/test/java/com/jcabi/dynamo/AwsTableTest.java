/*
 * Copyright (c) 2012-2025, jcabi.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 1) Redistributions of source code must retain the above
 * copyright notice, this list of conditions and the following
 * disclaimer. 2) Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided
 * with the distribution. 3) Neither the name of the jcabi.com nor
 * the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
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
