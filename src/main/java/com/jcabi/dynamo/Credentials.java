/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import java.net.URI;
import lombok.EqualsAndHashCode;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Amazon DynamoDB credentials.
 *
 * <p>It is recommended to use {@link Credentials.Simple} in most cases.
 *
 * @since 0.1
 */
@Immutable
@FunctionalInterface
public interface Credentials {

    /**
     * Test credentials, for unit testing mostly.
     */
    Credentials.Simple TEST = new Credentials.Simple(
        "AAAAAAAAAAAAAAAAAAAA",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    );

    /**
     * Build AWS client.
     *
     * <p>Don't forget to shut it down after use,
     * using {@link DynamoDbClient#close()}.
     *
     * @return Amazon Dynamo DB client
     */
    DynamoDbClient aws();

    /**
     * Simple implementation.
     *
     * @since 0.1
     */
    @Immutable
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = { "key", "secret", "region" })
    final class Simple implements Credentials {
        /**
         * AWS key.
         */
        private final transient String key;

        /**
         * AWS secret.
         */
        private final transient String secret;

        /**
         * Region name.
         */
        private final transient String region;

        /**
         * Public ctor, with "us-east-1" region.
         * @param akey AWS key
         * @param scrt Secret
         */
        public Simple(final String akey, final String scrt) {
            this(akey, scrt, Region.US_EAST_1.id());
        }

        /**
         * Public ctor.
         * @param akey AWS key
         * @param scrt Secret
         * @param reg Region
         */
        public Simple(final String akey, final String scrt, final String reg) {
            this.key = akey;
            this.secret = scrt;
            this.region = reg;
        }

        @Override
        public String toString() {
            return String.format("%s/%s", this.region, this.key);
        }

        @Override
        public DynamoDbClient aws() {
            return DynamoDbClient.builder()
                .region(Region.of(this.region))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(this.key, this.secret)
                    )
                )
                .build();
        }
    }

    /**
     * Assumed AWS IAM role.
     *
     * @see <a href="http://docs.aws.amazon.com/IAM/latest/UserGuide/role-usecase-ec2app.html">Granting Applications that Run on Amazon EC2 Instances Access to AWS Resources</a>
     * @since 0.1
     */
    @Immutable
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = "region")
    final class Assumed implements Credentials {
        /**
         * Region name.
         */
        private final transient String region;

        /**
         * Public ctor.
         */
        public Assumed() {
            this(Region.US_EAST_1.id());
        }

        /**
         * Public ctor.
         * @param reg Region
         */
        public Assumed(final String reg) {
            this.region = reg;
        }

        @Override
        public String toString() {
            return this.region;
        }

        @Override
        public DynamoDbClient aws() {
            return DynamoDbClient.builder()
                .region(Region.of(this.region))
                .build();
        }
    }

    /**
     * With explicitly specified endpoint.
     *
     * @since 0.1
     */
    @Immutable
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = { "origin", "endpoint" })
    final class Direct implements Credentials {
        /**
         * Original credentials.
         */
        private final transient Credentials.Simple origin;

        /**
         * Endpoint.
         */
        private final transient String endpoint;

        /**
         * Public ctor.
         * @param creds Original credentials
         * @param pnt Endpoint
         */
        public Direct(final Credentials.Simple creds, final String pnt) {
            this.origin = creds;
            this.endpoint = pnt;
        }

        /**
         * Public ctor.
         * @param creds Original credentials
         * @param port Port number for localhost
         */
        public Direct(final Credentials.Simple creds, final int port) {
            this(creds, String.format("http://localhost:%d", port));
        }

        @Override
        public String toString() {
            return String.format("%s at %s", this.origin, this.endpoint);
        }

        @Override
        public DynamoDbClient aws() {
            return DynamoDbClient.builder()
                .endpointOverride(URI.create(this.endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                            this.origin.key, this.origin.secret
                        )
                    )
                )
                .build();
        }
    }
}
