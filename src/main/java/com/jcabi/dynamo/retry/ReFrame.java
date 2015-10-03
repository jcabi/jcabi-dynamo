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
package com.jcabi.dynamo.retry;

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.aspects.Tv;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Table;
import com.jcabi.dynamo.Valve;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Frame that retries on failure.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
@SuppressWarnings("PMD.TooManyMethods")
public final class ReFrame implements Frame {

    /**
     * Original frame.
     */
    private final transient Frame origin;

    /**
     * Public ctor.
     * @param frame Origin frame
     */
    public ReFrame(
        @NotNull(message = "attribute frame cannot be null")
        final Frame frame) {
        this.origin = frame;
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    // @checkstyle AvoidDuplicateLiterals (1 line)
    @NotNull(message = "Frame cannot be null")
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public Frame where(
        @NotNull(message = "name cannot be null")
        final String name,
        @NotNull(message = "value cannot be null")
        final String value) {
        return new ReFrame(this.origin.where(name, value));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    // @checkstyle AvoidDuplicateLiterals (1 line)
    @NotNull(message = "Frame cannot be null")
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public Frame where(
        @NotNull(message = "attribute name cannot be null")
        final String name,
        @NotNull(message = "attribute condition cannot be null")
        final Condition condition) {
        return new ReFrame(this.origin.where(name, condition));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    // @checkstyle AvoidDuplicateLiterals (1 line)
    @NotNull(message = "Frame cannot be null")
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public Frame where(
        @NotNull(message = "attribute conditions cannot be null")
        final Map<String, Condition> conditions) {
        return new ReFrame(this.origin.where(conditions));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @NotNull(message = "Table cannot be null")
    public Table table() {
        return new ReTable(this.origin.table());
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    // @checkstyle AvoidDuplicateLiterals (1 line)
    @NotNull(message = "Frame cannot be null")
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public Frame through(
        @NotNull(message = "attribute valve cannot be null")
        final Valve valve) {
        return new ReFrame(this.origin.through(new ReValve(valve)));
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public int size() {
        return this.origin.size();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public boolean isEmpty() {
        return this.origin.isEmpty();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public boolean contains(
        @NotNull(message = "attribute obj cannot be null")
        final Object obj) {
        return this.origin.contains(obj);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @NotNull(message = "Iterator cannot be null")
    public Iterator<Item> iterator() {
        return new ReIterator<Item>(this.origin.iterator());
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @NotNull(message = "Object[] cannot be null")
    public Object[] toArray() {
        return this.origin.toArray();
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @NotNull(message = "array cannot be null")
    public <T> T[] toArray(
        @NotNull(message = "attribute arr cannot be null")
        final T[] arr) {
        return this.origin.toArray(arr);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public boolean add(
        @NotNull(message = "attribute item cannot be null")
        final Item item) {
        return this.origin.add(item);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public boolean remove(
        @NotNull(message = "obj cannot be null")
        final Object obj) {
        return this.origin.remove(obj);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public boolean containsAll(
        // @checkstyle AvoidDuplicateLiterals (1 line)
        @NotNull(message = "attribute list cannot be null")
        final Collection<?> list) {
        return this.origin.containsAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public boolean addAll(
        // @checkstyle AvoidDuplicateLiterals (1 line)
        @NotNull(message = "attribute list cannot be null")
        final Collection<? extends Item> list) {
        return this.origin.addAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public boolean removeAll(
        // @checkstyle AvoidDuplicateLiterals (1 line)
        @NotNull(message = "attribute list cannot be null")
        final Collection<?> list) {
        return this.origin.removeAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @SuppressWarnings("PMD.AvoidDuplicateLiterals")
    public boolean retainAll(
        // @checkstyle AvoidDuplicateLiterals (1 line)
        @NotNull(message = "attribute list cannot be null")
        final Collection<?> list) {
        return this.origin.retainAll(list);
    }

    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    public void clear() {
        this.origin.clear();
    }

}
