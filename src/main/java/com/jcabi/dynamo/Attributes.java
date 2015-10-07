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
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.ArrayMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.validation.constraints.NotNull;
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
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 */
@Immutable
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "attrs")
@SuppressWarnings({
    "PMD.TooManyMethods",
    "PMD.AvoidInstantiatingObjectsInLoops"
})
public final class Attributes implements Map<String, AttributeValue> {

    /**
     * Encapsulated attributes.
     */
    private final transient ArrayMap<String, AttributeValue> attrs;

    /**
     * Private ctor.
     */
    public Attributes() {
        this(new ArrayMap<String, AttributeValue>());
    }

    /**
     * Private ctor.
     * @param map Map of them
     */
    public Attributes(@NotNull(message = "map of attributes can't be NULL")
        final Map<String, AttributeValue> map) {
        this.attrs = new ArrayMap<String, AttributeValue>(map);
    }

    /**
     * With this attribute.
     * @param name Attribute name
     * @param value The value
     * @return Attributes
     */
    @NotNull(message = "Attributes cannot be null")
    public Attributes with(
        @NotNull(message = "the attribute name can't be NULL")
        final String name,
        @NotNull(message = "the value cannot be NULL")
        final AttributeValue value) {
        return new Attributes(
            this.attrs.with(String.format(Locale.ENGLISH, name), value)
        );
    }

    /**
     * With these attributes.
     * @param map Attributes to add
     * @return Attributes
     */
    @NotNull(message = "Attributes cannot be null")
    public Attributes with(
        @NotNull(message = "the map of attributes can't be NULL")
        final Map<String, AttributeValue> map) {
        final ConcurrentMap<String, AttributeValue> attribs =
            new ConcurrentHashMap<String, AttributeValue>(map.size());
        for (final Map.Entry<String, AttributeValue> entry : map.entrySet()) {
            attribs.put(
                String.format(Locale.ENGLISH, entry.getKey()),
                entry.getValue()
            );
        }
        return new Attributes(this.attrs.with(attribs));
    }

    /**
     * Convert them to a map of expected values.
     * @return Expected values
     */
    @NotNull(message = "Map cannot be null")
    public Map<String, ExpectedAttributeValue> asKeys() {
        final ImmutableMap.Builder<String, ExpectedAttributeValue> map =
            new ImmutableMap.Builder<String, ExpectedAttributeValue>();
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
    @NotNull(message = "The Attributes cannot be null")
    public Attributes with(
        @NotNull(message = "attribute name can't be null")
        final String name,
        @NotNull(message = "value can't be NULL")
        final Object value) {
        final AttributeValue attr;
        if (value instanceof Long || value instanceof Integer) {
            attr = new AttributeValue().withN(value.toString());
        } else {
            attr = new AttributeValue(value.toString());
        }
        return this.with(name, attr);
    }

    /**
     * Filter out all keys except provided ones.
     * @param keys Keys to leave in the map
     * @return Attributes
     */
    @NotNull(message = "Attributes cannot be null")
    public Attributes only(@NotNull(message = "the key names can't be NULL")
        final Collection<String> keys) {
        final ImmutableMap.Builder<String, AttributeValue> map =
            new ImmutableMap.Builder<String, AttributeValue>();
        final Collection<String> hash = new HashSet<String>(keys.size());
        for (final String key : keys) {
            hash.add(String.format(Locale.ENGLISH, key));
        }
        for (final Map.Entry<String, AttributeValue> entry : this.entrySet()) {
            if (hash.contains(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return new Attributes(map.build());
    }

    @Override
    @NotNull(message = "String cannot be null")
    public String toString() {
        final Collection<String> terms =
            new ArrayList<String>(this.attrs.size());
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
    public boolean containsKey(
        @NotNull(message = "attribute key cannot be NULL")
        final Object key) {
        return this.attrs.containsKey(
            String.format(Locale.ENGLISH, key.toString())
        );
    }

    @Override
    public boolean containsValue(
        @NotNull(message = "attribute value cannot be NULL")
        final Object value) {
        return this.attrs.containsValue(value);
    }

    @Override
    @NotNull(message = "AttributeValue cannot be null")
    public AttributeValue get(
        @NotNull(message = "attribute key cannot be NULL")
        final Object key) {
        return this.attrs.get(
            String.format(Locale.ENGLISH, key.toString())
        );
    }

    @Override
    @NotNull(message = "Set cannot be null")
    public Set<String> keySet() {
        return this.attrs.keySet();
    }

    @Override
    @NotNull(message = "Collection cannot be null")
    public Collection<AttributeValue> values() {
        return this.attrs.values();
    }

    @Override
    @NotNull(message = "Set cannot be null")
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
