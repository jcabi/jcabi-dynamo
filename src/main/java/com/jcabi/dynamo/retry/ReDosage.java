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
import com.jcabi.dynamo.Dosage;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Dosage that retries on failure.
 *
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
public final class ReDosage implements Dosage {

    /**
     * Original dosage.
     */
    private final transient Dosage origin;

    /**
     * Public ctor.
     * @param dosage Origin dosage
     */
    public ReDosage(final Dosage dosage) {
        this.origin = dosage;
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public List<Map<String, AttributeValue>> items() {
        return this.origin.items();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public boolean hasNext() {
        return this.origin.hasNext();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public Dosage next() {
        return new ReDosage(this.origin.next());
    }
}
