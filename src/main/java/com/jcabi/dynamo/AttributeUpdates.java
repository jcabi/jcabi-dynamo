/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
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
