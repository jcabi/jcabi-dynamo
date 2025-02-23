/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Amazon DynamoDB region.
 *
 * <p>It is recommended to use {@link Region.Simple} in most cases.
 *
 * <p>You can use {@link #aws()} method to get access to Amazon DynamoDB
 * client directly.
 *
 * <p>Since version 0.9 it is strongly recommended to wrap your region
 * in {@link com.jcabi.dynamo.retry.ReRegion} before use, for example:
 *
 * <pre> Region region = new ReRegion(new Region.Simple(credentials));</pre>
 *
 * <p>After all operations with the region are finished, it can be optionally
 * shutdown invoking {@link AmazonDynamoDB#shutdown()}. Callers are not expected
 * to call it, but can if they want to explicitly release any open resources and
 * forcibly terminate all pending asynchronous service calls. Once a client has
 * been shutdown, it should not be used to make any more requests.
 *
 * @since 0.1
 */
@Immutable
public interface Region {

    /**
     * Get DynamoDB client.
     * @return The client
     */
    AmazonDynamoDB aws();

    /**
     * Get one table.
     * @param name Table name
     * @return Table
     */
    Table table(String name);

    /**
     * Simple region, basic implementation.
     *
     * @since 0.1
     */
    @Immutable
    @Loggable(Loggable.DEBUG)
    @ToString
    @EqualsAndHashCode(of = "credentials")
    final class Simple implements Region {
        /**
         * Credentials.
         */
        private final transient Credentials credentials;

        /**
         * Public ctor.
         * @param creds Credentials
         */
        public Simple(final Credentials creds) {
            this.credentials = creds;
        }

        @Override
        public AmazonDynamoDB aws() {
            return this.credentials.aws();
        }

        @Override
        public Table table(final String name) {
            return new AwsTable(this.credentials, this, name);
        }
    }

    /**
     * All tables have a prefix in front of their names.
     *
     * <p>The region has to be used in combination with another region,
     * for example {@link Region.Simple}:
     *
     * <pre>Region region = new Region.Prefixed(
     *   new Region.Simple(creds),
     *   "foo-"
     * );</pre>
     *
     * <p>Now, {@code region.table("test")} will return a {@link Table}
     * instance pointing to the Dynamo DB table named {@code "foo-test"}. Could
     * be a convenient mechanism when you have many tables for different
     * projects in the same region.
     *
     * @since 0.1
     */
    @Immutable
    @Loggable(Loggable.DEBUG)
    @ToString
    @EqualsAndHashCode(of = { "origin", "prefix" })
    final class Prefixed implements Region {
        /**
         * Original region.
         */
        private final transient Region origin;

        /**
         * Prefix to add.
         */
        private final transient String prefix;

        /**
         * Public ctor.
         * @param region Original region
         * @param pfx Prefix to add to all tables
         */
        public Prefixed(final Region region, final String pfx) {
            this.origin = region;
            this.prefix = pfx;
        }

        @Override
        public AmazonDynamoDB aws() {
            return this.origin.aws();
        }

        @Override
        public Table table(final String name) {
            return this.origin.table(
                new StringBuilder(this.prefix).append(name).toString()
            );
        }
    }

}
