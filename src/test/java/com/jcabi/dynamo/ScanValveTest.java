/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
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
    void fetchesData() throws Exception {
        final Valve valve = new ScanValve();
        final Credentials credentials = Mockito.mock(Credentials.class);
        final ImmutableMap<String, AttributeValue> item =
            new ImmutableMap.Builder<String, AttributeValue>()
                .build();
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
        final Dosage dosage = valve.fetch(
            credentials, "table",
            new Conditions(), new ArrayList<>(0)
        );
        MatcherAssert.assertThat("should not has next", dosage.hasNext(), Matchers.is(false));
        MatcherAssert.assertThat("should has items", dosage.items(), Matchers.hasItem(item));
    }

}
