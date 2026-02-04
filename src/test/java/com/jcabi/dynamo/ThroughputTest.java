/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

/**
 * Test case for {@link Throughput}.
 * @since 0.1
 */
final class ThroughputTest {

    @Test
    void adjustsThroughput() {
        final Table table = Mockito.mock(Table.class);
        final Region region = Mockito.mock(Region.class);
        final DynamoDbClient aws = Mockito.mock(DynamoDbClient.class);
        Mockito.when(table.region()).thenReturn(region);
        final String name = "Customers";
        Mockito.when(table.name()).thenReturn(name);
        Mockito.when(region.aws()).thenReturn(aws);
        new Throughput(table).adjust();
        Mockito.verify(aws, Mockito.times(1))
            .updateTable(Mockito.any(UpdateTableRequest.class));
    }
}
