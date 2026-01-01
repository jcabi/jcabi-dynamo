/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link Throughput}.
 * @since 0.1
 */
final class ThroughputTest {

    @Test
    void adjustsThroughput() {
        final Table table = Mockito.mock(Table.class);
        final Region region = Mockito.mock(Region.class);
        final AmazonDynamoDB aws = Mockito.mock(AmazonDynamoDB.class);
        Mockito.when(table.region()).thenReturn(region);
        final String name = "Customers";
        Mockito.when(table.name()).thenReturn(name);
        Mockito.when(region.aws()).thenReturn(aws);
        new Throughput(table).adjust();
        Mockito.verify(aws, Mockito.times(1))
            .updateTable(
                Mockito.eq(name),
                Mockito.any()
            );
    }
}
