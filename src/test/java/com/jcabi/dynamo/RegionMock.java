/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.jcabi.dynamo.mock.MadeTable;
import com.jcabi.dynamo.retry.ReRegion;

/**
 * Mock of a {@link Region}.
 * @since 0.16.2
 */
final class RegionMock {

    /**
     * Dynamo table hash key.
     */
    private final transient String ahash;

    /**
     * Dynamo table range key.
     */
    private final transient String arrange;

    /**
     * DynamoDB Local port.
     */
    private final transient int prt;

    /**
     * Ctor.
     */
    RegionMock() {
        this(
            "hash-key",
            "range-key",
            Integer.parseInt(
                System.getProperty("failsafe.port")
            )
        );
    }

    /**
     * Ctor.
     * @param hash Hash
     * @param range Range
     * @param port Port
     */
    RegionMock(final String hash, final String range, final int port) {
        this.ahash = hash;
        this.arrange = range;
        this.prt = port;
    }

    /**
     * Get DynamoDB server port.
     * @return TCP port
     */
    public int port() {
        return this.prt;
    }

    /**
     * Get hash of the table.
     * @return Hash attribute name
     */
    public String hash() {
        return this.ahash;
    }

    /**
     * Get range of the table.
     * @return Hash attribute name
     */
    public String range() {
        return this.arrange;
    }

    /**
     * Get region with a table.
     * @param table Table name
     * @return Region
     * @throws Exception If fails
     */
    public Region get(final String table) throws Exception {
        final Region region = new Region.Simple(
            new Credentials.Direct(Credentials.TEST, this.prt)
        );
        final MadeTable mocker = new MadeTable(
            region,
            new CreateTableRequest()
                .withTableName(table)
                .withProvisionedThroughput(
                    new ProvisionedThroughput()
                        .withReadCapacityUnits(1L)
                        .withWriteCapacityUnits(1L)
                )
                .withAttributeDefinitions(
                    new AttributeDefinition()
                        .withAttributeName(this.ahash)
                        .withAttributeType(ScalarAttributeType.S),
                    new AttributeDefinition()
                        .withAttributeName(this.arrange)
                        .withAttributeType(ScalarAttributeType.N)
                )
                .withKeySchema(
                    new KeySchemaElement()
                        .withAttributeName(this.ahash)
                        .withKeyType(KeyType.HASH),
                    new KeySchemaElement()
                        .withAttributeName(this.arrange)
                        .withKeyType(KeyType.RANGE)
                )
        );
        mocker.create();
        mocker.createIfAbsent();
        return new ReRegion(region);
    }

}
