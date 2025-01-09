/*
 * Copyright (c) 2012-2025, jcabi.com
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

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Frame through AWS SDK.
 *
 * @since 0.1
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode
    (
        callSuper = false,
        of = { "credentials", "tbl", "name", "conditions", "valve" }
    )
final class AwsFrame extends AbstractCollection<Item> implements Frame {

    /**
     * AWS credentials.
     */
    private final transient Credentials credentials;

    /**
     * Table.
     */
    private final transient AwsTable tbl;

    /**
     * Table name.
     */
    private final transient String name;

    /**
     * Conditions.
     */
    private final transient Conditions conditions;

    /**
     * Valve with items.
     */
    private final transient Valve valve;

    /**
     * Public ctor.
     * @param creds Credentials
     * @param table Table
     * @param label Table name
     */
    AwsFrame(final Credentials creds, final AwsTable table,
        final String label) {
        this(creds, table, label, new Conditions(), new ScanValve());
    }

    /**
     * Public ctor.
     * @param creds Credentials
     * @param table Table
     * @param label Table name
     * @param conds Conditions
     * @param vlv Valve
     * @checkstyle ParameterNumber (5 lines)
     */
    AwsFrame(final Credentials creds, final AwsTable table,
        final String label, final Conditions conds, final Valve vlv) {
        super();
        this.credentials = creds;
        this.tbl = table;
        this.name = label;
        this.conditions = conds;
        this.valve = vlv;
    }

    @Override
    public boolean isEmpty() {
        return !this.iterator().hasNext();
    }

    @Override
    public Iterator<Item> iterator() {
        try {
            return new AwsIterator(
                this.credentials,
                this,
                this.name,
                this.conditions,
                this.tbl.keys(),
                this.valve
            );
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public int size() {
        try {
            return this.valve.count(
                this.credentials, this.name, this.conditions
            );
        } catch (final IOException ex) {
            throw new IllegalStateException(
                String.format("Can't count items in \"%s\"", this.name),
                ex
            );
        }
    }

    @Override
    public Frame where(final String attr, final String value) {
        return this.where(attr, Conditions.equalTo(value));
    }

    @Override
    public Frame where(final String attr, final Condition condition) {
        return new AwsFrame(
            this.credentials,
            this.tbl,
            this.name,
            this.conditions.with(attr, condition),
            this.valve
        );
    }

    @Override
    public Frame where(final Map<String, Condition> conds) {
        return new AwsFrame(
            this.credentials,
            this.tbl,
            this.name,
            this.conditions.with(conds),
            this.valve
        );
    }

    @Override
    public Frame through(final Valve vlv) {
        return new AwsFrame(
            this.credentials,
            this.tbl,
            this.name,
            this.conditions,
            vlv
        );
    }

    @Override
    public Table table() {
        return this.tbl;
    }

}
