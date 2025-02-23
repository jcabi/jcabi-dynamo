/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.base.Joiner;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.ArrayMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.EqualsAndHashCode;

/**
 * DynamoDB query conditions.
 *
 * <p>It's a convenient immutable builder of a map of conditions for
 * DynamoDB query/scan operations. Use it like this:
 *
 * <pre>Map&lt;String, Condition&gt; conditions = new Conditions()
 *   .with("hash", Conditions.equalTo("some value"))
 *   .with("range", Conditions.equalTo(12345));
 * </pre>
 *
 * @since 0.1
 */
@Immutable
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "conds")
@SuppressWarnings
    (
        {
            "PMD.TooManyMethods",
            "PMD.ProhibitPublicStaticMethods",
            "PMD.AvoidInstantiatingObjectsInLoops"
        }
    )
public final class Conditions implements Map<String, Condition> {

    /**
     * Pairs.
     */
    private final transient ArrayMap<String, Condition> conds;

    /**
     * Public ctor.
     */
    public Conditions() {
        this(new ArrayMap<>());
    }

    /**
     * Public ctor.
     * @param map Map of them
     */
    public Conditions(final Map<String, Condition> map) {
        this.conds = Conditions.array(map);
    }

    /**
     * Equal to static condition builder (factory method).
     * @param value The value to equal to
     * @return The condition just created
     */
    public static Condition equalTo(final Long value) {
        return Conditions.equalTo(
            new AttributeValue().withN(value.toString())
        );
    }

    /**
     * Equal to static condition builder (factory method).
     * @param value The value to equal to
     * @return The condition just created
     */
    public static Condition equalTo(final Integer value) {
        return Conditions.equalTo(
            new AttributeValue().withN(value.toString())
        );
    }

    /**
     * Equal to static condition builder (factory method).
     * @param value The value to equal to
     * @return The condition just created
     */
    public static Condition equalTo(final Object value) {
        return Conditions.equalTo(
            new AttributeValue().withS(value.toString())
        );
    }

    /**
     * Equal to static condition builder (factory method).
     * @param value The value to equal to
     * @return The condition just created
     */
    public static Condition equalTo(final AttributeValue value) {
        return new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(value);
    }

    /**
     * With this condition.
     * @param name Attribute name
     * @param value The condition
     * @return New map of conditions
     */
    public Conditions with(final String name, final Condition value) {
        return new Conditions(
            this.conds.with(name, value)
        );
    }

    /**
     * With this condition.
     * @param name Attribute name
     * @param value The condition
     * @return New map of conditions
     * @since 0.18
     */
    public Conditions with(final String name, final Object value) {
        return new Conditions(
            this.conds.with(
                name,
                Conditions.equalTo(value)
            )
        );
    }

    /**
     * With these conditions.
     * @param map The conditions
     * @return New map of conditions
     */
    public Conditions withAttributes(final Map<String, AttributeValue> map) {
        final ConcurrentMap<String, Condition> cnds =
            new ConcurrentHashMap<>(map.size());
        for (final Map.Entry<String, AttributeValue> entry : map.entrySet()) {
            cnds.put(
                entry.getKey(),
                Conditions.equalTo(entry.getValue())
            );
        }
        return new Conditions(this.conds.with(cnds));
    }

    /**
     * With these conditions.
     * @param map The conditions
     * @return New map of conditions
     */
    public Conditions with(final Map<String, Condition> map) {
        return new Conditions(this.conds.with(map));
    }

    @Override
    public String toString() {
        final Collection<String> terms =
            new ArrayList<>(this.conds.size());
        for (final Map.Entry<String, Condition> cond : this.conds.entrySet()) {
            terms.add(
                String.format(
                    "%s %s %s",
                    cond.getKey(),
                    cond.getValue().getComparisonOperator(),
                    cond.getValue().getAttributeValueList()
                )
            );
        }
        return Joiner.on(" AND ").join(terms);
    }

    @Override
    public int size() {
        return this.conds.size();
    }

    @Override
    public boolean isEmpty() {
        return this.conds.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return this.conds.containsKey(key.toString());
    }

    @Override
    public boolean containsValue(final Object value) {
        return this.conds.containsValue(value);
    }

    @Override
    public Condition get(final Object key) {
        return this.conds.get(key.toString());
    }

    @Override
    public Set<String> keySet() {
        return this.conds.keySet();
    }

    @Override
    public Collection<Condition> values() {
        return this.conds.values();
    }

    @Override
    public Set<Map.Entry<String, Condition>> entrySet() {
        return this.conds.entrySet();
    }

    @Override
    public Condition put(final String key, final Condition value) {
        throw new UnsupportedOperationException(
            "Conditions class is immutable, can't do #put()"
        );
    }

    @Override
    public Condition remove(final Object key) {
        throw new UnsupportedOperationException(
            "Conditions class is immutable, can't do #remove()"
        );
    }

    @Override
    public void putAll(final Map<? extends String, ? extends Condition> map) {
        throw new UnsupportedOperationException(
            "Conditions class is immutable, can't do #putAll()"
        );
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(
            "Conditions class is immutable, can't do #clear()"
        );
    }

    /**
     * Convert map to ArrayMap.
     * @param map Map of them
     * @return Array map
     */
    private static ArrayMap<String, Condition> array(
        final Map<String, Condition> map) {
        final ConcurrentMap<String, Condition> cnds =
            new ConcurrentHashMap<>(map.size());
        cnds.putAll(map);
        return new ArrayMap<>(cnds);
    }

}
