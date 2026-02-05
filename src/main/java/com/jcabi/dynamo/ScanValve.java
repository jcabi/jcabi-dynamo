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
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

/**
 * Scan-based valve.
 *
 * @since 0.1
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "limit", "attributes" })
public final class ScanValve implements Valve {

    /**
     * Limit to use for every query.
     */
    private final transient int limit;

    /**
     * Attributes to fetch.
     */
    @Immutable.Array
    private final transient String[] attributes;

    /**
     * Public ctor.
     */
    public ScanValve() {
        this(100, new ArrayList<>(0));
    }

    /**
     * Public ctor.
     * @param lmt Limit
     * @param attrs Attributes to pre-load
     */
    private ScanValve(final int lmt, final Iterable<String> attrs) {
        this.limit = lmt;
        this.attributes = Iterables.toArray(attrs, String.class);
    }

    // @checkstyle ParameterNumber (5 lines)
    @Override
    public Dosage fetch(final Credentials credentials,
        final String table, final Map<String, Condition> conditions,
        final Collection<String> keys) throws IOException {
        final DynamoDbClient aws = credentials.aws();
        try {
            final Collection<String> attrs = new HashSet<>(
                Arrays.asList(this.attributes)
            );
            attrs.addAll(keys);
            final ScanRequest request = ScanRequest.builder()
                .tableName(table)
                .attributesToGet(attrs)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .scanFilter(conditions)
                .limit(this.limit)
                .build();
            final ScanResponse result = aws.scan(request);
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
            return new ScanValve.NextDosage(credentials, request, result);
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
        final Map<String, Condition> conditions) {
        final DynamoDbClient aws = credentials.aws();
        try {
            final ScanRequest request = ScanRequest.builder()
                .tableName(table)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .scanFilter(conditions)
                .select(Select.COUNT)
                .limit(Integer.MAX_VALUE)
                .build();
            final ScanResponse result = aws.scan(request);
            Logger.info(
                this,
                // @checkstyle LineLength (1 line)
                "#total(): COUNT=%d in '%s' using %s, %s",
                result.count(), request.tableName(),
                request.filterExpression(),
                new PrintableConsumedCapacity(
                    result.consumedCapacity()
                ).print()
            );
            return result.count();
        } finally {
            aws.close();
        }
    }

    /**
     * With given limit.
     * @param lmt Limit to use
     * @return New query valve
     */
    public ScanValve withLimit(final int lmt) {
        return new ScanValve(lmt, Arrays.asList(this.attributes));
    }

    /**
     * With this extra attribute to pre-fetch.
     * @param name Name of attribute to pre-load
     * @return New query valve
     */
    public ScanValve withAttributeToGet(final String name) {
        return new ScanValve(
            this.limit,
            Iterables.concat(
                Arrays.asList(this.attributes),
                Collections.singletonList(name)
            )
        );
    }

    /**
     * With these extra attributes to pre-fetch.
     * @param names Name of attributes to pre-load
     * @return New query valve
     */
    public ScanValve withAttributeToGet(final String... names) {
        return new ScanValve(
            this.limit,
            Iterables.concat(
                Arrays.asList(this.attributes),
                Arrays.asList(names)
            )
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
         * Scan request.
         */
        private final transient ScanRequest request;

        /**
         * Scan response.
         */
        private final transient ScanResponse result;

        /**
         * Public ctor.
         * @param creds Credentials
         * @param rqst Scan request
         * @param rslt Scan response
         */
        NextDosage(final Credentials creds, final ScanRequest rqst,
            final ScanResponse rslt) {
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
                final ScanRequest rqst = this.request.toBuilder()
                    .exclusiveStartKey(
                        this.result.lastEvaluatedKey()
                    )
                    .build();
                final ScanResponse rslt = aws.scan(rqst);
                Logger.info(
                    this,
                    // @checkstyle LineLength (1 line)
                    "#next(): loaded %d item(s) from '%s' and stopped at %s, using %s, %s",
                    rslt.count(), rqst.tableName(), rqst.scanFilter(),
                    rslt.lastEvaluatedKey(),
                    new PrintableConsumedCapacity(
                        rslt.consumedCapacity()
                    ).print()
                );
                return new ScanValve.NextDosage(this.credentials, rqst, rslt);
            } finally {
                aws.close();
            }
        }
    }
}
