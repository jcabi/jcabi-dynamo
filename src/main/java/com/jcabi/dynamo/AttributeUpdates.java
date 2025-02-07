/*
 * Copyright (c) 2012-2025 Yegor Bugayenko
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
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.google.common.base.Joiner;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.ArrayMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;

/**
 * DynamoDB item attribute updates.
 *
 * @since 0.12
 */
@Immutable
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "attrs")
@SuppressWarnings
    (
        {
            "PMD.TooManyMethods",
            "PMD.AvoidInstantiatingObjectsInLoops"
        }
    )
public final class AttributeUpdates
    implements Map<String, AttributeValueUpdate> {

    /**
     * Encapsulated AttributeUpdates.
     */
    private final transient ArrayMap<String, AttributeValueUpdate> attrs;

    /**
     * Private ctor.
     */
    public AttributeUpdates() {
        this(new ArrayMap<>());
    }

    /**
     * Private ctor.
     * @param map Map of them
     */
    public AttributeUpdates(final Map<String, AttributeValueUpdate> map) {
        this.attrs = new ArrayMap<>(map);
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return AttributeUpdates
     */
    public AttributeUpdates with(final String name,
        final AttributeValueUpdate value) {
        return new AttributeUpdates(
            this.attrs.with(name, value)
        );
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return AttributeUpdates
     * @since 0.14.3
     */
    public AttributeUpdates with(final String name,
        final AttributeValue value) {
        return this.with(
            name,
            new AttributeValueUpdate(value, AttributeAction.PUT)
        );
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return AttributeUpdates
     * @since 0.14.3
     */
    public AttributeUpdates with(final String name, final Object value) {
        final AttributeValue attr;
        if (value instanceof Long || value instanceof Integer) {
            attr = new AttributeValue().withN(value.toString());
        } else {
            attr = new AttributeValue(value.toString());
        }
        return this.with(name, attr);
    }

    /**
     * With these AttributeUpdates.
     * @param map AttributeUpdates to add
     * @return AttributeUpdates
     */
    public AttributeUpdates with(final Map<String, AttributeValueUpdate> map) {
        final Map<String, AttributeValueUpdate> attribs =
            new ConcurrentHashMap<>(map.size());
        attribs.putAll(map);
        return new AttributeUpdates(this.attrs.with(attribs));
    }

    @Override
    public String toString() {
        final Collection<String> terms =
            new ArrayList<>(this.attrs.size());
        for (final Map.Entry<String, AttributeValueUpdate> attr
            : this.attrs.entrySet()) {
            terms.add(
                String.format(
                    "%s=%s",
                    attr.getKey(),
                    attr.getValue()
                )
            );
        }
        return Joiner.on("; ").join(terms);
    }

    @Override
    public int size() {
        return this.attrs.size();
    }

    @Override
    public boolean isEmpty() {
        return this.attrs.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return this.attrs.containsKey(key.toString());
    }

    @Override
    public boolean containsValue(final Object value) {
        return this.attrs.containsValue(value);
    }

    @Override
    public AttributeValueUpdate get(final Object key) {
        return this.attrs.get(key.toString());
    }

    @Override
    public Set<String> keySet() {
        return this.attrs.keySet();
    }

    @Override
    public Collection<AttributeValueUpdate> values() {
        return this.attrs.values();
    }

    @Override
    public Set<Map.Entry<String, AttributeValueUpdate>> entrySet() {
        return this.attrs.entrySet();
    }

    @Override
    public AttributeValueUpdate put(final String key,
        final AttributeValueUpdate value) {
        throw new UnsupportedOperationException(
            "AttributeUpdates class is immutable, can't do #put()"
        );
    }

    @Override
    public AttributeValueUpdate remove(final Object key) {
        throw new UnsupportedOperationException(
            "AttributeUpdates class is immutable, can't do #remove()"
        );
    }

    @Override
    public void putAll(
        final Map<? extends String, ? extends AttributeValueUpdate> map) {
        throw new UnsupportedOperationException(
            "AttributeUpdates class is immutable, can't do #putAll()"
        );
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(
            "AttributeUpdates class is immutable, can't do #clear()"
        );
    }
}
