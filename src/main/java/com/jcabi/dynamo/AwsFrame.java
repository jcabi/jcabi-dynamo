/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Frame through AWS SDK.
 *
 * @since 0.1
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
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
    @SuppressWarnings("PMD.LooseCoupling")
    public boolean equals(final Object obj) {
        final boolean equal;
        if (this == obj) {
            equal = true;
        } else if (obj instanceof AwsFrame) {
            final AwsFrame other = (AwsFrame) obj;
            equal = Objects.equals(this.credentials, other.credentials)
                && Objects.equals(this.tbl, other.tbl)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.conditions, other.conditions)
                && Objects.equals(this.valve, other.valve);
        } else {
            equal = false;
        }
        return equal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            this.credentials, this.tbl, this.name, this.conditions, this.valve
        );
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
