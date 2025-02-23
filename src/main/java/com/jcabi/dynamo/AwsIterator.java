/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.Array;
import com.jcabi.log.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Iterator of items in AWS SDK.
 *
 * <p>The class is mutable and thread-safe.
 *
 * @since 0.1
 */
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode
    (
        of = { "credentials", "conditions", "frame", "name", "keys", "valve" }
    )
@SuppressWarnings("PMD.GuardLogStatement")
final class AwsIterator implements Iterator<Item> {

    /**
     * AWS credentials.
     */
    private final transient Credentials credentials;

    /**
     * Conditions.
     */
    private final transient Conditions conditions;

    /**
     * Frame.
     */
    private final transient AwsFrame frame;

    /**
     * Table name.
     */
    private final transient String name;

    /**
     * List of primary keys in the table.
     */
    private final transient Collection<String> keys;

    /**
     * Valve that loads dosages of items.
     */
    private final transient Valve valve;

    /**
     * Last scan result (mutable).
     */
    private final transient AtomicReference<Dosage> dosage;

    /**
     * Position inside the scan result, last seen, starts with -1 (mutable).
     */
    private transient int position;

    /**
     * Public ctor.
     * @param creds Credentials
     * @param frm Frame object
     * @param label Table name
     * @param conds Conditions
     * @param primary Primary keys of the table
     * @param vlv Valve with items
     * @checkstyle ParameterNumber (5 lines)
     */
    AwsIterator(final Credentials creds, final AwsFrame frm,
        final String label, final Conditions conds,
        final Collection<String> primary, final Valve vlv) {
        this.credentials = creds;
        this.frame = frm;
        this.name = label;
        this.conditions = conds;
        this.keys = primary;
        this.valve = vlv;
        this.dosage = new AtomicReference<>();
        this.position = -1;
    }

    @Override
    public boolean hasNext() {
        synchronized (this.dosage) {
            if (this.dosage.get() == null) {
                try {
                    this.dosage.set(
                        this.valve.fetch(
                            this.credentials,
                            this.name,
                            this.conditions,
                            this.keys
                        )
                    );
                } catch (final IOException ex) {
                    throw new IllegalStateException(ex);
                }
                this.position = -1;
            }
            if (this.dosage.get().hasNext()
                && this.position + 1 >= this.dosage.get().items().size()) {
                this.dosage.set(this.dosage.get().next());
                this.position = -1;
            }
            return this.dosage.get().items().size() - this.position > 1;
        }
    }

    @Override
    public Item next() {
        synchronized (this.dosage) {
            if (!this.hasNext()) {
                throw new NoSuchElementException(
                    String.format(
                        "No more items in the frame, position=%d",
                        this.position
                    )
                );
            }
            ++this.position;
            return new AwsItem(
                this.credentials,
                this.frame,
                this.name,
                new Attributes(this.dosage.get().items().get(this.position)),
                new Array<>(this.keys)
            );
        }
    }

    @Override
    @SuppressWarnings("PMD.UseConcurrentHashMap")
    public void remove() {
        synchronized (this.dosage) {
            final Dosage prev = this.dosage.get();
            if (prev == null) {
                throw new IllegalStateException(
                    "You can't call remove() until you call next()"
                );
            }
            final AmazonDynamoDB aws = this.credentials.aws();
            try {
                final List<Map<String, AttributeValue>> items =
                    new ArrayList<>(prev.items());
                final Map<String, AttributeValue> item =
                    items.remove(this.position);
                final long start = System.currentTimeMillis();
                final DeleteItemResult res = aws.deleteItem(
                    new DeleteItemRequest()
                        .withTableName(this.name)
                        .withKey(new Attributes(item).only(this.keys))
                        .withReturnConsumedCapacity(
                            ReturnConsumedCapacity.TOTAL
                        )
                        .withExpected(
                            new Attributes(item).only(this.keys).asKeys()
                        )
                );
                this.dosage.set(new AwsIterator.Fixed(prev, items));
                --this.position;
                Logger.info(
                    this,
                    "#remove(): item #%d removed from DynamoDB, %s, in %[ms]s",
                    this.position,
                    new PrintableConsumedCapacity(
                        res.getConsumedCapacity()
                    ).print(),
                    System.currentTimeMillis() - start
                );
            } finally {
                aws.shutdown();
            }
        }
    }

    /**
     * Dosage with fixed list of items.
     *
     * @since 0.1
     */
    @Immutable
    private static final class Fixed implements Dosage {
        /**
         * List of items.
         */
        private final transient Array<Map<String, AttributeValue>> list;

        /**
         * Previous dosage.
         */
        private final transient Dosage prev;

        /**
         * Ctor.
         * @param dsg Dosage
         * @param items Items
         */
        Fixed(final Dosage dsg, final List<Map<String, AttributeValue>> items) {
            this.prev = dsg;
            this.list = new Array<>(items);
        }

        @Override
        public List<Map<String, AttributeValue>> items() {
            return Collections.unmodifiableList(this.list);
        }

        @Override
        public Dosage next() {
            return this.prev.next();
        }

        @Override
        public boolean hasNext() {
            return this.prev.hasNext();
        }
    }
}
