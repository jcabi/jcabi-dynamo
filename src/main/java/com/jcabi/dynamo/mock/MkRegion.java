/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Mock version of {@link Region}.
 *
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "data")
public final class MkRegion implements Region {

    /**
     * Data.
     */
    private final transient MkData data;

    /**
     * Public ctor.
     * @param dta Data to use
     */
    public MkRegion(final MkData dta) {
        this.data = dta;
    }

    @Override
    public AmazonDynamoDB aws() {
        throw new UnsupportedOperationException(
            "direct access to AWS DynamoDB client is not supported by MkRegion"
        );
    }

    @Override
    public Table table(final String name) {
        return new MkTable(this.data, name);
    }

}
