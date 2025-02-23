/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.jcabi.aspects.Tv;

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
     * @param tbl The table.
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
            .aws()
            .updateTable(
                this.table.name(),
                Throughput.suitableThroughput()
            );
    }

    /**
     * Decides which throughput value is the most suitable according to
     * certain parameters of elasticity/scalability.
     * @return Throughput settings.
     * @todo #10 The exact algorithm for figuring out read and write
     *  capacities should be based on a CloudWatch metric accessed with
     *  credentials of this.table.region().aws().
     */
    private static ProvisionedThroughput suitableThroughput() {
        return new ProvisionedThroughput(
            (long) Tv.HUNDRED,
            (long) Tv.HUNDRED
        );
    }
}
