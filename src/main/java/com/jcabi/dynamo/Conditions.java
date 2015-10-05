/**
 * Copyright (c) 2012-2015, jcabi.com
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
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.base.Joiner;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.ArrayMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.validation.constraints.NotNull;
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
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 */
@Immutable
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "conds")
@SuppressWarnings({
    "PMD.TooManyMethods",
    "PMD.AvoidInstantiatingObjectsInLoops"
})
public final class Conditions implements Map<String, Condition> {

    /**
     * Pairs.
     */
    private final transient ArrayMap<String, Condition> conds;

    /**
     * Public ctor.
     */
    public Conditions() {
        this(new ArrayMap<String, Condition>());
    }

    /**
     * Public ctor.
     * @param map Map of them
     */
    public Conditions(
        @NotNull(message="Attribute map cannot be null")
        final Map<String, Condition> map) {
        final ConcurrentMap<String, Condition> cnds =
            new ConcurrentHashMap<String, Condition>(map.size());
        for (final Map.Entry<String, Condition> entry : map.entrySet()) {
            cnds.put(
                String.format(Locale.ENGLISH, entry.getKey()),
                entry.getValue()
            );
        }
        this.conds = new ArrayMap<String, Condition>(cnds);
    }

    /**
     * Equal to static condition builder (factory method).
     * @param value The value to equal to
     * @return The condition just created
     */
    @NotNull
    public static Condition equalTo(
        @NotNull(message = "Attribute value cannot be null")
        final Object value) {
        final AttributeValue attr;
        if (value instanceof Long || value instanceof Integer) {
            attr = new AttributeValue().withN(value.toString());
        } else {
            attr = new AttributeValue().withS(value.toString());
        }
        return Conditions.equalTo(attr);
    }

    /**
     * Equal to static condition builder (factory method).
     * @param value The value to equal to
     * @return The condition just created
     */
    @NotNull
    public static Condition equalTo(
        @NotNull(message = "Attribute value cannot be null")
        final AttributeValue value) {
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
    @NotNull
    public Conditions with(
        @NotNull(message = "Attribute name cannot be null")
        final String name,
        @NotNull(message = "Attribute value cannot be null")
        final Condition value) {
        return new Conditions(
            this.conds.with(String.format(Locale.ENGLISH, name), value)
        );
    }

    /**
     * With this condition.
     * @param name Attribute name
     * @param value The condition
     * @return New map of conditions
     * @since 0.18
     */
    @NotNull(message = "Conditions cannot be null")
    public Conditions with(
        @NotNull(message = "Attribute name cannot be null")
        final String name,
        @NotNull(message = "Attribute value cannot be null")
        final Object value) {
        return new Conditions(
            this.conds.with(
                String.format(Locale.ENGLISH, name),
                Conditions.equalTo(value)
            )
        );
    }

    /**
     * With these conditions.
     * @param map The conditions
     * @return New map of conditions
     */
    @NotNull(message = "Conditions cannot be null")
    public Conditions with(
            @NotNull(message = "attribute map cannot be null")
            final Map<String, Condition> map) {
        return new Conditions(this.conds.with(map));
    }

    @Override
    @NotNull(message = "String cannot be null")
    public String toString() {
        final Collection<String> terms =
            new ArrayList<String>(this.conds.size());
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
    public boolean containsKey(
        @NotNull(message = "Attribute key cannot be null")
        final Object key) {
        return this.conds.containsKey(
            String.format(Locale.ENGLISH, key.toString())
        );
    }

    @Override
    public boolean containsValue(
        @NotNull(message = "Attribute value cannot be null")
        final Object value) {
        return this.conds.containsValue(value);
    }

    @Override
    @NotNull(message = "Condition cannot be null")
    public Condition get(
        @NotNull(message = "Attribute value cannot be null")
        final Object key) {
        return this.conds.get(
            String.format(Locale.ENGLISH, key.toString())
        );
    }

    @Override
    @NotNull(message = "Set cannot be null")
    public Set<String> keySet() {
        return this.conds.keySet();
    }

    @Override
    @NotNull(message = "Collection cannot be null")
    public Collection<Condition> values() {
        return this.conds.values();
    }

    @Override
    @NotNull(message = "Set cannot be null")
    public Set<Map.Entry<String, Condition>> entrySet() {
        return this.conds.entrySet();
    }

    @Override
    @NotNull(message = "Condition cannot be null")
    public Condition put(
        @NotNull(message = "Attribute key cannot be null")
        final String key,
        @NotNull(message = "Attribute value cannot be null")
        final Condition value) {
        throw new UnsupportedOperationException(
            "Conditions class is immutable, can't do #put()"
        );
    }

    @Override
    @NotNull(message = "Condition cannot be null")
    public Condition remove(
        @NotNull(message = "Attribute key cannot be null")
        final Object key) {
        throw new UnsupportedOperationException(
            "Conditions class is immutable, can't do #remove()"
        );
    }

    @Override
    public void putAll(
        @NotNull(message = "Attribute map cannot be null")
        final Map<? extends String, ? extends Condition> map) {
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

}
