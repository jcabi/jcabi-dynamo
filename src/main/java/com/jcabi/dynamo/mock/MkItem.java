/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

/**
 * Mock version of {@link Item}.
 *
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "data", "table", "attributes" })
final class MkItem implements Item {

    /**
     * Data.
     */
    private final transient MkData data;

    /**
     * Table name.
     */
    private final transient String table;

    /**
     * Attributes.
     */
    private final transient Attributes attributes;

    /**
     * Public ctor.
     * @param dta Data
     * @param tbl Table
     * @param attribs Map of attributes
     */
    MkItem(final MkData dta, final String tbl, final Attributes attribs) {
        this.data = dta;
        this.table = tbl;
        this.attributes = attribs;
    }

    @Override
    public AttributeValue get(final String name) throws IOException {
        return this.data.iterate(
            this.table,
            new Conditions().withAttributes(
                this.attributes.only(this.data.keys(this.table))
            )
        ).iterator().next().get(name);
    }

    @Override
    public boolean has(final String name) throws IOException {
        return this.data.iterate(
            this.table,
            new Conditions().withAttributes(
                this.attributes.only(this.data.keys(this.table))
            )
        ).iterator().next().containsKey(name.toLowerCase(Locale.ENGLISH));
    }

    @Override
    public Map<String, AttributeValue> put(
        final String name, final AttributeValueUpdate value)
        throws IOException {
        return this.put(
            new ImmutableMap.Builder<String, AttributeValueUpdate>()
                .put(name, value).build()
        );
    }

    @Override
    public Map<String, AttributeValue> put(
        final Map<String, AttributeValueUpdate> attrs) throws IOException {
        final Map<String, AttributeValue> keys =
            new HashMap<>(0);
        for (final String attr : this.data.keys(this.table)) {
            keys.put(attr, this.attributes.get(attr));
        }
        try {
            this.data.update(
                this.table,
                new Attributes(keys),
                new AttributeUpdates(attrs)
            );
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
        return Maps.transformValues(
            attrs,
            new Function<AttributeValueUpdate, AttributeValue>() {
                @Override
                public AttributeValue apply(final AttributeValueUpdate update) {
                    return update.value();
                }
            }
        );
    }

    @Override
    public Frame frame() {
        return new MkFrame(this.data, this.table);
    }
}
