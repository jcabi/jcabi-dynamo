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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.ArrayMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.EqualsAndHashCode;

/**
 * DynamoDB item attributes.
 *
 * <p>It's a convenient immutable builder of a map of attribute values for
 * DynamoDB put operation. Use it like this:
 *
 * <pre>Map&lt;String, AttributeValue&gt; attributes = new Attributes()
 *   .with("hash", "some value")
 *   .with("range", 12345);
 * </pre>
 *
 * @since 0.1
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
public final class Attributes implements Map<String, AttributeValue> {

    /**
     * Encapsulated attributes.
     */
    private final transient ArrayMap<String, AttributeValue> attrs;

    /**
     * Private ctor.
     */
    public Attributes() {
        this(new ArrayMap<>());
    }

    /**
     * Private ctor.
     * @param map Map of them
     */
    public Attributes(final Map<String, AttributeValue> map) {
        this.attrs = new ArrayMap<>(map);
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return Attributes
     */
    public Attributes with(final String name, final AttributeValue value) {
        return new Attributes(this.attrs.with(name, value));
    }

    /**
     * With these attributes.
     * @param map Attributes to add
     * @return Attributes
     */
    public Attributes with(final Map<String, AttributeValue> map) {
        final ConcurrentMap<String, AttributeValue> attribs =
            new ConcurrentHashMap<>(map.size());
        for (final Map.Entry<String, AttributeValue> entry : map.entrySet()) {
            attribs.put(entry.getKey(), entry.getValue());
        }
        return new Attributes(this.attrs.with(attribs));
    }

    /**
     * Convert them to a map of expected values.
     * @return Expected values
     */
    public Map<String, ExpectedAttributeValue> asKeys() {
        final ImmutableMap.Builder<String, ExpectedAttributeValue> map =
            new ImmutableMap.Builder<>();
        for (final Map.Entry<String, AttributeValue> attr
            : this.attrs.entrySet()) {
            map.put(
                attr.getKey(),
                new ExpectedAttributeValue(attr.getValue())
            );
        }
        return map.build();
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return Attributes
     * @
     */
    public Attributes with(final String name, final Long value) {
        return this.with(name, new AttributeValue().withN(value.toString()));
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return Attributes
     * @
     */
    public Attributes with(final String name, final Integer value) {
        return this.with(name, new AttributeValue().withN(value.toString()));
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return Attributes
     * @
     */
    public Attributes with(final String name, final Object value) {
        return this.with(name, new AttributeValue(value.toString()));
    }

    /**
     * Filter out all keys except provided ones.
     * @param keys Keys to leave in the map
     * @return Attributes
     */
    public Attributes only(final Iterable<String> keys) {
        final ImmutableMap.Builder<String, AttributeValue> map =
            new ImmutableMap.Builder<>();
        final Collection<String> hash = new HashSet<>(0);
        for (final String key : keys) {
            hash.add(key);
        }
        for (final Map.Entry<String, AttributeValue> entry : this.entrySet()) {
            if (hash.contains(entry.getKey())) {
                map.put(
                    entry.getKey(),
                    entry.getValue()
                );
            }
        }
        return new Attributes(map.build());
    }

    @Override
    public String toString() {
        final Collection<String> terms =
            new ArrayList<>(this.attrs.size());
        for (final Map.Entry<String, AttributeValue> attr
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
    public AttributeValue get(final Object key) {
        return this.attrs.get(key.toString());
    }

    @Override
    public Set<String> keySet() {
        return this.attrs.keySet();
    }

    @Override
    public Collection<AttributeValue> values() {
        return this.attrs.values();
    }

    @Override
    public Set<Map.Entry<String, AttributeValue>> entrySet() {
        return this.attrs.entrySet();
    }

    @Override
    public AttributeValue put(final String key, final AttributeValue value) {
        throw new UnsupportedOperationException(
            "Attributes class is immutable, can't do #put()"
        );
    }

    @Override
    public AttributeValue remove(final Object key) {
        throw new UnsupportedOperationException(
            "Attributes class is immutable, can't do #remove()"
        );
    }

    @Override
    public void putAll(
        final Map<? extends String, ? extends AttributeValue> map) {
        throw new UnsupportedOperationException(
            "Attributes class is immutable, can't do #putAll()"
        );
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(
            "Attributes class is immutable, can't do #clear()"
        );
    }
}
