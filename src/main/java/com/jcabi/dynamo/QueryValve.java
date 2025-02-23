/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.google.common.collect.Iterables;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.Tv;
import com.jcabi.log.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Query-based valve.
 *
 * @since 0.1
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "limit", "forward" })
@SuppressWarnings({"PMD.TooManyMethods", "PMD.GuardLogStatement"})
public final class QueryValve implements Valve {

    /**
     * Limit to use for every query.
     */
    private final transient int limit;

    /**
     * Forward/reverse order.
     */
    private final transient boolean forward;

    /**
     * Attributes to fetch.
     */
    @Immutable.Array
    private final transient String[] attributes;

    /**
     * Index name.
     */
    private final transient String index;

    /**
     * What attributes to select.
     */
    private final transient String select;

    /**
     * Consistent read.
     */
    private final transient boolean consistent;

    /**
     * Public ctor.
     */
    public QueryValve() {
        this(
            Tv.TWENTY, true, new ArrayList<>(0),
            "", Select.SPECIFIC_ATTRIBUTES.toString(), true
        );
    }

    /**
     * Public ctor.
     * @param lmt Limit
     * @param fwd Forward
     * @param attrs Names of attributes to pre-fetch
     * @param idx Index name or empty string
     * @param slct Select
     * @param cnst Consistent read
     * @checkstyle ParameterNumber (5 lines)
     */
    private QueryValve(final int lmt, final boolean fwd,
        final Iterable<String> attrs, final String idx,
        final String slct, final boolean cnst) {
        this.limit = lmt;
        this.forward = fwd;
        this.attributes = Iterables.toArray(attrs, String.class);
        this.index = idx;
        this.select = slct;
        this.consistent = cnst;
    }

    // @checkstyle ParameterNumber (5 lines)
    @Override
    public Dosage fetch(final Credentials credentials, final String table,
        final Map<String, Condition> conditions, final Collection<String> keys)
        throws IOException {
        final AmazonDynamoDB aws = credentials.aws();
        try {
            final Collection<String> attrs = new HashSet<>(
                Arrays.asList(this.attributes)
            );
            attrs.addAll(keys);
            QueryRequest request = new QueryRequest()
                .withTableName(table)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withKeyConditions(conditions)
                .withConsistentRead(this.consistent)
                .withScanIndexForward(this.forward)
                .withSelect(this.select)
                .withLimit(this.limit);
            if (this.select.equals(Select.SPECIFIC_ATTRIBUTES.toString())) {
                request = request.withAttributesToGet(attrs);
            }
            if (!this.index.isEmpty()) {
                request = request.withIndexName(this.index);
            }
            final long start = System.currentTimeMillis();
            final QueryResult result = aws.query(request);
            Logger.info(
                this,
                // @checkstyle LineLength (1 line)
                "#items(): loaded %d item(s) from '%s' and stopped at %s, using %s, %s, in %[ms]s",
                result.getCount(), table,
                result.getLastEvaluatedKey(),
                conditions,
                new PrintableConsumedCapacity(
                    result.getConsumedCapacity()
                ).print(),
                System.currentTimeMillis() - start
            );
            return new QueryValve.NextDosage(credentials, request, result);
        } catch (final AmazonClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to fetch from \"%s\" by %s and %s",
                    table, conditions, keys
                ),
                ex
            );
        } finally {
            aws.shutdown();
        }
    }

    @Override
    public int count(final Credentials credentials, final String table,
        final Map<String, Condition> conditions) throws IOException {
        final AmazonDynamoDB aws = credentials.aws();
        try {
            QueryRequest request = new QueryRequest()
                .withTableName(table)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withKeyConditions(conditions)
                .withConsistentRead(this.consistent)
                .withSelect(Select.COUNT)
                .withLimit(Integer.MAX_VALUE);
            if (!this.index.isEmpty()) {
                request = request.withIndexName(this.index);
            }
            final long start = System.currentTimeMillis();
            final QueryResult rslt = aws.query(request);
            final int count = rslt.getCount();
            Logger.info(
                this,
                // @checkstyle LineLength (1 line)
                "#total(): COUNT=%d in '%s' using %s, %s, in %[ms]s",
                count, request.getTableName(), request.getQueryFilter(),
                new PrintableConsumedCapacity(
                    rslt.getConsumedCapacity()
                ).print(),
                System.currentTimeMillis() - start
            );
            return count;
        } catch (final AmazonClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to count from \"%s\" by %s",
                    table, conditions
                ),
                ex
            );
        } finally {
            aws.shutdown();
        }
    }

    /**
     * With consistent read.
     * @param cnst Consistent read
     * @return New query valve
     * @see QueryRequest#withConsistentRead(Boolean)
     * @since 0.12
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withConsistentRead(final boolean cnst) {
        return new QueryValve(
            this.limit, this.forward,
            Arrays.asList(this.attributes),
            this.index, this.select, cnst
        );
    }

    /**
     * With index name.
     * @param idx Index name
     * @return New query valve
     * @see QueryRequest#withIndexName(String)
     * @since 0.10.2
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withIndexName(final String idx) {
        return new QueryValve(
            this.limit, this.forward,
            Arrays.asList(this.attributes),
            idx, this.select, this.consistent
        );
    }

    /**
     * With attributes to select.
     * @param slct Select to use
     * @return New query valve
     * @see QueryRequest#withSelect(Select)
     * @since 0.10.2
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withSelect(final Select slct) {
        return new QueryValve(
            this.limit, this.forward,
            Arrays.asList(this.attributes), this.index,
            slct.toString(), this.consistent
        );
    }

    /**
     * With given limit.
     * @param lmt Limit to use
     * @return New query valve
     * @see QueryRequest#withLimit(Integer)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withLimit(final int lmt) {
        return new QueryValve(
            lmt, this.forward,
            Arrays.asList(this.attributes),
            this.index, this.select, this.consistent
        );
    }

    /**
     * With scan index forward flag.
     * @param fwd Forward flag
     * @return New query valve
     * @see QueryRequest#withScanIndexForward(Boolean)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withScanIndexForward(final boolean fwd) {
        return new QueryValve(
            this.limit, fwd,
            Arrays.asList(this.attributes),
            this.index, this.select, this.consistent
        );
    }

    /**
     * With this extra attribute to pre-fetch.
     * @param name Name of attribute to pre-load
     * @return New query valve
     * @see QueryRequest#withAttributesToGet(Collection)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withAttributeToGet(final String name) {
        return new QueryValve(
            this.limit, this.forward,
            Iterables.concat(
                Arrays.asList(this.attributes),
                Collections.singleton(name)
            ),
            this.index, this.select, this.consistent
        );
    }

    /**
     * With these extra attributes to pre-fetch.
     * @param names Name of attributes to pre-load
     * @return New query valve
     * @see QueryRequest#withAttributesToGet(Collection)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    public QueryValve withAttributesToGet(final String... names) {
        return new QueryValve(
            this.limit, this.forward,
            Iterables.concat(
                Arrays.asList(this.attributes),
                Arrays.asList(names)
            ),
            this.index,
            this.select, this.consistent
        );
    }

    /**
     * Next dosage.
     *
     * @since 0.1
     */
    @ToString
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = { "credentials", "request", "result" })
    private final class NextDosage implements Dosage {
        /**
         * AWS client.
         */
        private final transient Credentials credentials;

        /**
         * Query request.
         */
        private final transient QueryRequest request;

        /**
         * Query request.
         */
        private final transient QueryResult result;

        /**
         * Public ctor.
         * @param creds Credentials
         * @param rqst Query request
         * @param rslt Query result
         */
        NextDosage(final Credentials creds, final QueryRequest rqst,
            final QueryResult rslt) {
            this.credentials = creds;
            this.request = rqst;
            this.result = rslt;
        }

        @Override
        public List<Map<String, AttributeValue>> items() {
            return this.result.getItems();
        }

        @Override
        public boolean hasNext() {
            return this.result.getLastEvaluatedKey() != null;
        }

        @Override
        public Dosage next() {
            if (!this.hasNext()) {
                throw new IllegalStateException(
                    "Nothing left in the iterator"
                );
            }
            final AmazonDynamoDB aws = this.credentials.aws();
            try {
                final QueryRequest rqst =
                    this.request.withExclusiveStartKey(
                        this.result.getLastEvaluatedKey()
                    );
                final long start = System.currentTimeMillis();
                final QueryResult rslt = aws.query(rqst);
                Logger.info(
                    this,
                    // @checkstyle LineLength (1 line)
                    "#next(): loaded %d item(s) from '%s' and stopped at %s, using %s, %s, in %[ms]s",
                    rslt.getCount(), rqst.getTableName(),
                    rslt.getLastEvaluatedKey(),
                    rqst.getKeyConditions(),
                    new PrintableConsumedCapacity(
                        rslt.getConsumedCapacity()
                    ).print(),
                    System.currentTimeMillis() - start
                );
                return new QueryValve.NextDosage(this.credentials, rqst, rslt);
            } finally {
                aws.shutdown();
            }
        }
    }
}
