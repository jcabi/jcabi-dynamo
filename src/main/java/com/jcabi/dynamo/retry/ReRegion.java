/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Region that retries on failure.
 *
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
public final class ReRegion implements Region {

    /**
     * Original region.
     */
    private final transient Region origin;

    /**
     * Public ctor.
     * @param region Origin region
     */
    public ReRegion(final Region region) {
        this.origin = region;
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public DynamoDbClient aws() {
        return this.origin.aws();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Table table(final String name) {
        return new ReTable(this.origin.table(name));
    }
}
