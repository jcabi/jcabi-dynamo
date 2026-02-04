/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.dynamo.mock.MadeTable;
import com.jcabi.dynamo.retry.ReRegion;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

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
            CreateTableRequest.builder()
                .tableName(table)
                .provisionedThroughput(
                    ProvisionedThroughput.builder()
                        .readCapacityUnits(1L)
                        .writeCapacityUnits(1L)
                        .build()
                )
                .attributeDefinitions(
                    AttributeDefinition.builder()
                        .attributeName(this.ahash)
                        .attributeType(ScalarAttributeType.S)
                        .build(),
                    AttributeDefinition.builder()
                        .attributeName(this.arrange)
                        .attributeType(ScalarAttributeType.N)
                        .build()
                )
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName(this.ahash)
                        .keyType(KeyType.HASH)
                        .build(),
                    KeySchemaElement.builder()
                        .attributeName(this.arrange)
                        .keyType(KeyType.RANGE)
                        .build()
                )
                .build()
        );
        mocker.create();
        mocker.createIfAbsent();
        return new ReRegion(region);
    }

}
