/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link ScanValve}.
 * @since 0.1
 */
final class ScanValveTest {

    @Test
    @SuppressWarnings("unchecked")
    void fetchesData() throws Exception {
        final Valve valve = new ScanValve();
        final Credentials credentials = Mockito.mock(Credentials.class);
        final ImmutableMap<String, AttributeValue> item =
            new ImmutableMap.Builder<String, AttributeValue>()
                .build();
        final AmazonDynamoDB aws = Mockito.mock(AmazonDynamoDB.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            new ScanResult()
                .withItems(
                    Collections.singletonList(item)
            )
                .withConsumedCapacity(
                    new ConsumedCapacity().withCapacityUnits(1d)
                )
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        final Dosage dosage = valve.fetch(
            credentials, "table",
            new Conditions(), new ArrayList<>(0)
        );
        MatcherAssert.assertThat("should not has next", dosage.hasNext(), Matchers.is(false));
        MatcherAssert.assertThat("should has items", dosage.items(), Matchers.hasItem(item));
    }

}
