/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Frame that retries on failure.
 *
 * @param <T> Type of items
 * @since 0.9
 */
@ToString
@EqualsAndHashCode(of = "origin")
@Loggable(Loggable.DEBUG)
public final class ReIterator<T> implements Iterator<T> {

    /**
     * Original iterator.
     */
    private final transient Iterator<T> origin;

    /**
     * Public ctor.
     * @param iterator Origin iterator
     */
    public ReIterator(final Iterator<T> iterator) {
        this.origin = iterator;
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean hasNext() {
        return this.origin.hasNext();
    }

    @Override
    @RetryOnFailure
        (
            verbose = false, delay = 5, unit = TimeUnit.SECONDS,
            ignore = NoSuchElementException.class
        )
    public T next() {
        return this.origin.next();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public void remove() {
        this.origin.remove();
    }
}
