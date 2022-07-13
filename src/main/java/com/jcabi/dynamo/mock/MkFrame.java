/**
 * Copyright (c) 2012-2022, jcabi.com
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
