/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

/**
 * Throughput of a table.
 * @since 0.18.4
 */
public final class Throughput {

    /**
     * Table.
     */
    private final transient Table table;

    /**
     * Public ctor.
     * @param tbl The table
     */
    public Throughput(final Table tbl) {
        this.table = tbl;
    }

    /**
     * Adjusts throughput on the table.
     */
    public void adjust() {
        this.table
            .region()
            .aws().updateTable(
                UpdateTableRequest.builder()
                    .tableName(this.table.name())
                    .provisionedThroughput(Throughput.suitableThroughput())
                    .build()
            );
    }

    private static ProvisionedThroughput suitableThroughput() {
        return ProvisionedThroughput.builder()
            .readCapacityUnits(100L)
            .writeCapacityUnits(100L)
            .build();
    }
}
