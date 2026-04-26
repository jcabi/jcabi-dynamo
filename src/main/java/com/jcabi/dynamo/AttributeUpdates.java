/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

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
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

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
        "PMD.LooseCoupling"
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
            AttributeValueUpdate.builder()
                .value(value)
                .action(AttributeAction.PUT)
                .build()
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
        return new AttributeUpdates(
            this.attrs.with(name, AttributeUpdates.toUpdate(value))
        );
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
        return this.attrs.containsValue(AttributeUpdates.toUpdate(value));
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

    /**
     * Convert any input value to an {@link AttributeValueUpdate}.
     *
     * <p>If the value is already an {@link AttributeValueUpdate}, it is
     * returned as-is. If it is an {@link AttributeValue}, it is wrapped
     * with a {@link AttributeAction#PUT} action. Numeric values are
     * stored as {@code n}, anything else as {@code s} via
     * {@link Object#toString()}.
     *
     * @param value The value to convert
     * @return The converted {@link AttributeValueUpdate}
     */
    private static AttributeValueUpdate toUpdate(final Object value) {
        final AttributeValueUpdate result;
        if (value instanceof AttributeValueUpdate) {
            result = (AttributeValueUpdate) value;
        } else if (value instanceof AttributeValue) {
            result = AttributeValueUpdate.builder()
                .value((AttributeValue) value)
                .action(AttributeAction.PUT)
                .build();
        } else {
            final AttributeValue attr;
            if (value instanceof Long || value instanceof Integer) {
                attr = AttributeValue.builder().n(value.toString()).build();
            } else {
                attr = AttributeValue.builder().s(value.toString()).build();
            }
            result = AttributeValueUpdate.builder()
                .value(attr)
                .action(AttributeAction.PUT)
                .build();
        }
        return result;
    }
}
