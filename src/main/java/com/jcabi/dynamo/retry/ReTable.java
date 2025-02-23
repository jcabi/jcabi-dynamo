/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.aspects.Tv;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Table that retries on failure.
 *
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
public final class ReTable implements Table {

    /**
     * Original table.
     */
    private final transient Table origin;

    /**
     * Public ctor.
     * @param table Origin table
     */
    public ReTable(final Table table) {
        this.origin = table;
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public Item put(final Map<String, AttributeValue> attributes)
        throws IOException {
        return this.origin.put(attributes);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public Frame frame() {
        return new ReFrame(this.origin.frame());
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public Region region() {
        return new ReRegion(this.origin.region());
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public String name() {
        return this.origin.name();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public void delete(final Map<String, AttributeValue> attributes)
        throws IOException {
        this.origin.delete(attributes);
    }
}
