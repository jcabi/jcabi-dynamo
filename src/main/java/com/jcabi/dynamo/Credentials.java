/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import lombok.EqualsAndHashCode;

/**
 * Amazon DynamoDB credentials.
 *
 * <p>It is recommended to use {@link Credentials.Simple} in most cases.
 *
 * @since 0.1
 */
@Immutable
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
     * using {@link AmazonDynamoDB#shutdown()}.
     *
     * @return Amazon Dynamo DB client
     */
    AmazonDynamoDB aws();

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
            this(akey, scrt, Regions.US_EAST_1.getName());
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
        public AmazonDynamoDB aws() {
            final com.amazonaws.regions.Region reg =
                RegionUtils.getRegion(this.region);
            if (reg == null) {
                throw new IllegalStateException(
                    String.format("Failed to find region '%s'", this.region)
                );
            }
            return AmazonDynamoDBClientBuilder.standard()
                .withRegion(reg.getName())
                .withCredentials(
                    new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(this.key, this.secret)
                    )
                )
                .build();
        }
    }

    /**
     * Assumed AWS IAM role.
     *
     * @since 0.1
     * @see <a href="http://docs.aws.amazon.com/IAM/latest/UserGuide/role-usecase-ec2app.html">Granting Applications that Run on Amazon EC2 Instances Access to AWS Resources</a>
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
            this(Regions.US_EAST_1.getName());
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
        public AmazonDynamoDB aws() {
            final com.amazonaws.regions.Region reg =
                RegionUtils.getRegion(this.region);
            if (reg == null) {
                throw new IllegalStateException(
                    String.format("Failed to detect region '%s'", this.region)
                );
            }
            return AmazonDynamoDBClientBuilder.standard()
                .withRegion(reg.getName())
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
        public AmazonDynamoDB aws() {
            return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                        this.endpoint, Regions.US_EAST_1.getName()
                    )
                )
                .withCredentials(
                    new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                            this.origin.key, this.origin.secret
                        )
                    )
                )
                .build();
        }
    }
}
