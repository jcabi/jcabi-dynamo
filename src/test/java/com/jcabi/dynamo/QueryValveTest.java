/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link QueryValve}.
 * @since 0.1
 */
final class QueryValveTest {

    @Test
    @SuppressWarnings("unchecked")
    void fetchesData() throws Exception {
        final Valve valve = new QueryValve();
        final Credentials credentials = Mockito.mock(Credentials.class);
        final ImmutableMap<String, AttributeValue> item =
            new ImmutableMap.Builder<String, AttributeValue>()
                .build();
        final AmazonDynamoDB aws = Mockito.mock(AmazonDynamoDB.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            new QueryResult()
                .withItems(
                    Collections.singletonList(item)
            )
                .withConsumedCapacity(
                    new ConsumedCapacity().withCapacityUnits(1.0d)
                )
        ).when(aws).query(Mockito.any(QueryRequest.class));
        final Dosage dosage = valve.fetch(
            credentials, "table",
            new Conditions(), new ArrayList<>(0)
        );
        MatcherAssert.assertThat(dosage.hasNext(), Matchers.is(false));
        MatcherAssert.assertThat(dosage.items(), Matchers.hasItem(item));
    }

}
