/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Table;
import com.jcabi.dynamo.Valve;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Frame that retries on failure.
 *
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
@SuppressWarnings("PMD.TooManyMethods")
public final class ReFrame implements Frame {

    /**
     * Original frame.
     */
    private final transient Frame origin;

    /**
     * Public ctor.
     * @param frame Origin frame
     */
    public ReFrame(final Frame frame) {
        this.origin = frame;
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Frame where(final String name, final String value) {
        return new ReFrame(this.origin.where(name, value));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Frame where(final String name, final Condition condition) {
        return new ReFrame(this.origin.where(name, condition));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Frame where(final Map<String, Condition> conditions) {
        return new ReFrame(this.origin.where(conditions));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Table table() {
        return new ReTable(this.origin.table());
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Frame through(final Valve valve) {
        return new ReFrame(this.origin.through(new ReValve(valve)));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public int size() {
        return this.origin.size();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean isEmpty() {
        return this.origin.isEmpty();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean contains(final Object obj) {
        return this.origin.contains(obj);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Iterator<Item> iterator() {
        return new ReIterator<>(this.origin.iterator());
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public Object[] toArray() {
        return this.origin.toArray();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public <T> T[] toArray(final T[] arr) {
        return this.origin.toArray(arr);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean add(final Item item) {
        return this.origin.add(item);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean remove(final Object obj) {
        return this.origin.remove(obj);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean containsAll(final Collection<?> list) {
        return this.origin.containsAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean addAll(final Collection<? extends Item> list) {
        return this.origin.addAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean removeAll(final Collection<?> list) {
        return this.origin.removeAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public boolean retainAll(final Collection<?> list) {
        return this.origin.retainAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = 5, unit = TimeUnit.SECONDS)
    public void clear() {
        this.origin.clear();
    }

}
