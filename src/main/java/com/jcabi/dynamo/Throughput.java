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
