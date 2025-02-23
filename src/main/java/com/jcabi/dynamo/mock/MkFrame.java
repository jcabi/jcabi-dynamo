/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Table;
import com.jcabi.dynamo.Valve;
import com.jcabi.immutable.ArrayMap;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Mock version of {@link Frame}.
 *
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(callSuper = false, of = { "tbl", "data", "conds" })
final class MkFrame extends AbstractCollection<Item> implements Frame {

    /**
     * Data.
     */
    private final transient MkData data;

    /**
     * Table.
     */
    private final transient String tbl;

    /**
     * Conditions.
     */
    private final transient Conditions conds;

    /**
     * Public ctor.
     * @param dta Data
     * @param table Table
     */
    MkFrame(final MkData dta, final String table) {
        this(dta, table, new Conditions());
    }

    /**
     * Public ctor.
     * @param dta Data
     * @param table Table
     * @param conditions Map of conditions
     */
    MkFrame(final MkData dta, final String table, final Conditions conditions) {
        super();
        this.data = dta;
        this.tbl = table;
        this.conds = conditions;
    }

    @Override
    public Iterator<Item> iterator() {
        try {
            return Iterators.transform(
                this.data.iterate(this.tbl, this.conds).iterator(),
                new Function<Attributes, Item>() {
                    @Override
                    public Item apply(final Attributes input) {
                        return new MkItem(
                            MkFrame.this.data,
                            MkFrame.this.tbl,
                            input
                        );
                    }
                }
            );
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public int size() {
        return Iterators.size(this.iterator());
    }

    @Override
    public Frame where(final String name, final String value) {
        return this.where(name, Conditions.equalTo(value));
    }

    @Override
    public Frame where(final String name, final Condition condition) {
        return this.where(
            new ArrayMap<String, Condition>().with(name, condition)
        );
    }

    @Override
    public Frame where(final Map<String, Condition> conditions) {
        return new MkFrame(this.data, this.tbl, this.conds.with(conditions));
    }

    @Override
    public Table table() {
        return new MkTable(this.data, this.tbl);
    }

    @Override
    public Frame through(final Valve valve) {
        return this;
    }
}
