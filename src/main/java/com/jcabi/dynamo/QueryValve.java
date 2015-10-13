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
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Query-based valve.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "limit", "forward" })
@SuppressWarnings("PMD.TooManyMethods")
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
            Tv.TWENTY, true, new ArrayList<String>(0),
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
    private QueryValve(
        @NotNull(message = "attribute lmt cannot be NULL")
        final int lmt,
        @NotNull(message = "attribute fwd cannot be NULL")
        final boolean fwd,
        @NotNull(message = "attribute attrs cannot be NULL")
        final Iterable<String> attrs,
        @NotNull(message = "attribute idx cannot be NULL")
        final String idx,
        @NotNull(message = "attribute slct cannot be NULL")
        final String slct,
        @NotNull(message = "attribute cnst cannot be NULL")
        final boolean cnst) {
        this.limit = lmt;
        this.forward = fwd;
        this.attributes = Iterables.toArray(attrs, String.class);
        this.index = idx;
        this.select = slct;
        this.consistent = cnst;
    }

    // @checkstyle ParameterNumber (5 lines)
    @Override
    @NotNull(message = "Dosage cannot be null")
    public Dosage fetch(
        @NotNull(message = "attribute credentials cannot be NULL")
        final Credentials credentials,
        @NotNull(message = "attribute table cannot be NULL")
        final String table,
        @NotNull(message = "attribute conditions cannot be NULL")
        final Map<String, Condition> conditions,
        @NotNull(message = "attribute keys canoot be NULL")
        final Collection<String> keys)
        throws IOException {
        final AmazonDynamoDB aws = credentials.aws();
        try {
            final Collection<String> attrs = new HashSet<String>(
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
                "#items(): loaded %d item(s) from '%s' using %s%s, in %[ms]s",
                result.getCount(), table, conditions,
                AwsTable.print(result.getConsumedCapacity()),
                System.currentTimeMillis() - start
            );
            return new QueryValve.NextDosage(credentials, request, result);
        } catch (final AmazonClientException ex) {
            throw new IOException(ex);
        } finally {
            aws.shutdown();
        }
    }

    /**
     * With consistent read.
     * @param cnst Consistent read
     * @return New query valve
     * @since 0.12
     * @see QueryRequest#withConsistentRead(Boolean)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    @NotNull(message = "This QueryValve cannot be null")
    public QueryValve withConsistentRead(
        @NotNull(message = "attribute cnst cannot be null")
        final boolean cnst) {
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
     * @since 0.10.2
     * @see QueryRequest#withIndexName(String)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    @NotNull(message = "QueryValve cannot be NULL.")
    public QueryValve withIndexName(
        @NotNull(message = "attribute idx cannot be NULL")
        final String idx) {
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
     * @since 0.10.2
     * @see QueryRequest#withSelect(Select)
     * @checkstyle AvoidDuplicateLiterals (5 line)
     */
    @NotNull(message = "QueryValve should be null")
    public QueryValve withSelect(
        @NotNull(message = "attribute slct cannot be NULL")
        final Select slct) {
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
    @NotNull(message = "That QueryValve cannot be null")
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
    @NotNull(message = "QueryValve cannot be NUll")
    public QueryValve withScanIndexForward(
        @NotNull(message = "attribute fwd cannot be NULL")
        final boolean fwd) {
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
    @NotNull(message = "QueryValve cannot be null")
    public QueryValve withAttributeToGet(
        @NotNull(message = "attribute name can't be NULL")
        final String name) {
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
    @NotNull(message = "the QueryValve cannot be null")
    public QueryValve withAttributesToGet(
        @NotNull(message = "attribute names cannot be NULL")
        final String... names) {
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
        NextDosage(
            @NotNull(message = "attribute creds cannot be NULL")
            final Credentials creds,
            @NotNull(message = "attribute rqst cannot be NULL")
            final QueryRequest rqst,
            @NotNull(message = "attribute rslt cannot be NULL")
            final QueryResult rslt) {
            this.credentials = creds;
            this.request = rqst;
            this.result = rslt;
        }
        @Override
        @NotNull(message = "List cannot be null")
        public List<Map<String, AttributeValue>> items() {
            return this.result.getItems();
        }
        @Override
        public boolean hasNext() {
            return this.result.getLastEvaluatedKey() != null;
        }
        @Override
        @NotNull(message = "next dosage cannot be null")
        public Dosage next() {
            if (!this.hasNext()) {
                throw new IllegalStateException(
                    "nothing left in the iterator"
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
                    "#next(): loaded %d item(s) from '%s' using %s%s, in %[ms]s",
                    rslt.getCount(), rqst.getTableName(),
                    rqst.getKeyConditions(),
                    AwsTable.print(rslt.getConsumedCapacity()),
                    System.currentTimeMillis() - start
                );
                return new QueryValve.NextDosage(this.credentials, rqst, rslt);
            } finally {
                aws.shutdown();
            }
        }
    }
}
