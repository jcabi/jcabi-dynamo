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
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.16.2
 * @checkstyle ClassDataAbstractionCouplingCheck (500 lines)
 */
final class RegionMock {

    /**
     * Dynamo table hash key.
     */
    public static final String HASH = "hash-key";

    /**
     * Dynamo table range key.
     */
    public static final String RANGE = "range-key";

    /**
     * DynamoDB Local port.
     */
    private static final int PORT = Integer.parseInt(
        System.getProperty("failsafe.port")
    );

    /**
     * Get region with a table.
     * @param table Table name
     * @return Region
     * @throws Exception If fails
     */
    public Region get(final String table) throws Exception {
        final Region region = new Region.Simple(
            new Credentials.Direct(Credentials.TEST, RegionMock.PORT)
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
                        .withAttributeName(RegionMock.HASH)
                        .withAttributeType(ScalarAttributeType.S),
                    new AttributeDefinition()
                        .withAttributeName(RegionMock.RANGE)
                        .withAttributeType(ScalarAttributeType.N)
                )
                .withKeySchema(
                    new KeySchemaElement()
                        .withAttributeName(RegionMock.HASH)
                        .withKeyType(KeyType.HASH),
                    new KeySchemaElement()
                        .withAttributeName(RegionMock.RANGE)
                        .withKeyType(KeyType.RANGE)
                )
        );
        mocker.create();
        mocker.createIfAbsent();
        return new ReRegion(region);
    }

}
