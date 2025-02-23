/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.aspects.Tv;
import com.jcabi.dynamo.Credentials;
import com.jcabi.dynamo.Dosage;
import com.jcabi.dynamo.Valve;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Valve that retries on failure.
 *
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
public final class ReValve implements Valve {

    /**
     * Original valve.
     */
    private final transient Valve origin;

    /**
     * Public ctor.
     * @param valve Origin valve
     */
    public ReValve(final Valve valve) {
        this.origin = valve;
    }

    // @checkstyle ParameterNumber (7 lines)
    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public Dosage fetch(final Credentials credentials, final String table,
        final Map<String, Condition> conditions,
        final Collection<String> keys) throws IOException {
        return new ReDosage(
            this.origin.fetch(credentials, table, conditions, keys)
        );
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public int count(final Credentials credentials, final String table,
        final Map<String, Condition> conditions) throws IOException {
        return this.origin.count(credentials, table, conditions);
    }

}
