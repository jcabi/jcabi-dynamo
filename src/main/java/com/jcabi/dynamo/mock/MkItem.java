/**
 * Copyright (c) 2012-2016, jcabi.com
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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
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

/**
 * Mock version of {@link Item}.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
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
            new HashMap<String, AttributeValue>(0);
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
                    return update.getValue();
                }
            }
        );
    }

    @Override
    public Frame frame() {
        return new MkFrame(this.data, this.table);
    }
}
