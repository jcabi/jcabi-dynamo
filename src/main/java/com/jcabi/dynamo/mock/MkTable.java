/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import java.io.IOException;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Mock version of {@link Table}.
 *
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "self", "data" })
final class MkTable implements Table {

    /**
     * Data.
     */
    private final transient MkData data;

    /**
     * Name of the table.
     */
    private final transient String self;

    /**
     * Public ctor.
     * @param dta Data
     * @param name Name of the table
     */
    MkTable(final MkData dta, final String name) {
        this.data = dta;
        this.self = name;
    }

    @Override
    public Item put(final Map<String, AttributeValue> attributes) {
        final Attributes attrs = new Attributes(attributes);
        try {
            this.data.put(this.self, attrs);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
        return new MkItem(this.data, this.self, attrs);
    }

    @Override
    public Frame frame() {
        return new MkFrame(this.data, this.self);
    }

    @Override
    public Region region() {
        return new MkRegion(this.data);
    }

    @Override
    public String name() {
        return this.self;
    }

    @Override
    public void delete(final Map<String, AttributeValue> attributes)
        throws IOException {
        this.data.delete(
            this.self,
            new Attributes(attributes)
        );
    }
}
