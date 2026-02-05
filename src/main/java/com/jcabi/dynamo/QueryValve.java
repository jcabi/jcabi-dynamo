/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.google.common.collect.Iterables;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
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
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.Select;

/**
 * Query-based valve.
 *
 * @since 0.1
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "limit", "forward" })
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
            20, true, new ArrayList<>(0),
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
        final DynamoDbClient aws = credentials.aws();
        try {
            final Collection<String> attrs = new HashSet<>(
                Arrays.asList(this.attributes)
            );
            attrs.addAll(keys);
            QueryRequest.Builder bld = QueryRequest.builder()
                .tableName(table)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .keyConditions(conditions)
                .consistentRead(this.consistent)
                .scanIndexForward(this.forward)
                .select(this.select)
                .limit(this.limit);
            if (this.select.equals(Select.SPECIFIC_ATTRIBUTES.toString())) {
                bld = bld.attributesToGet(attrs);
            }
            if (!this.index.isEmpty()) {
                bld = bld.indexName(this.index);
            }
            final QueryRequest request = bld.build();
            final QueryResponse result = aws.query(request);
            Logger.info(
                this,
                // @checkstyle LineLength (1 line)
                "#items(): loaded %d item(s) from '%s' and stopped at %s, using %s, %s",
                result.count(), table,
                result.lastEvaluatedKey(),
                conditions,
                new PrintableConsumedCapacity(
                    result.consumedCapacity()
                ).print()
            );
            return new QueryValve.NextDosage(credentials, request, result);
        } catch (final SdkClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to fetch from \"%s\" by %s and %s",
                    table, conditions, keys
                ),
                ex
            );
        } finally {
            aws.close();
        }
    }

    @Override
    public int count(final Credentials credentials, final String table,
        final Map<String, Condition> conditions) throws IOException {
        final DynamoDbClient aws = credentials.aws();
        try {
            QueryRequest.Builder bld = QueryRequest.builder()
                .tableName(table)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .keyConditions(conditions)
                .consistentRead(this.consistent)
                .select(Select.COUNT)
                .limit(Integer.MAX_VALUE);
            if (!this.index.isEmpty()) {
                bld = bld.indexName(this.index);
            }
            final QueryRequest request = bld.build();
            final QueryResponse rslt = aws.query(request);
            Logger.info(
                this,
                // @checkstyle LineLength (1 line)
                "#total(): COUNT=%d in '%s' using %s, %s",
                rslt.count(), request.tableName(), request.queryFilter(),
                new PrintableConsumedCapacity(
                    rslt.consumedCapacity()
                ).print()
            );
            return rslt.count();
        } catch (final SdkClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to count from \"%s\" by %s",
                    table, conditions
                ),
                ex
            );
        } finally {
            aws.close();
        }
    }

    /**
     * With consistent read.
     * @param cnst Consistent read
     * @return New query valve
     * @see QueryRequest#consistentRead()
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
     * @see QueryRequest#indexName()
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
     * @see QueryRequest#select()
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
     * @see QueryRequest#limit()
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
     * @see QueryRequest#scanIndexForward()
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
     * @see QueryRequest#attributesToGet()
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
     * @see QueryRequest#attributesToGet()
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
         * Query response.
         */
        private final transient QueryResponse result;

        /**
         * Public ctor.
         * @param creds Credentials
         * @param rqst Query request
         * @param rslt Query response
         */
        NextDosage(final Credentials creds, final QueryRequest rqst,
            final QueryResponse rslt) {
            this.credentials = creds;
            this.request = rqst;
            this.result = rslt;
        }

        @Override
        public List<Map<String, AttributeValue>> items() {
            return this.result.items();
        }

        @Override
        public boolean hasNext() {
            return !this.result.lastEvaluatedKey().isEmpty();
        }

        @Override
        public Dosage next() {
            if (!this.hasNext()) {
                throw new IllegalStateException(
                    "Nothing left in the iterator"
                );
            }
            final DynamoDbClient aws = this.credentials.aws();
            try {
                final QueryRequest rqst = this.request.toBuilder()
                    .exclusiveStartKey(
                        this.result.lastEvaluatedKey()
                    )
                    .build();
                final QueryResponse rslt = aws.query(rqst);
                Logger.info(
                    this,
                    // @checkstyle LineLength (1 line)
                    "#next(): loaded %d item(s) from '%s' and stopped at %s, using %s, %s",
                    rslt.count(), rqst.tableName(),
                    rslt.lastEvaluatedKey(),
                    rqst.keyConditions(),
                    new PrintableConsumedCapacity(
                        rslt.consumedCapacity()
                    ).print()
                );
                return new QueryValve.NextDosage(this.credentials, rqst, rslt);
            } finally {
                aws.close();
            }
        }
    }
}
