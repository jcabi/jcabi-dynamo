/**
 * Copyright (c) 2012-2013, JCabi.com
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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.jcabi.aspects.Tv;
import java.util.Iterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Integration case for {@link Region}.
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @checkstyle ClassDataAbstractionCoupling (500 lines)
 */
public final class RegionITCase {

    /**
     * DynamoDB Local port.
     */
    private static final int PORT = Integer.parseInt(
        System.getProperty("failsafe.port")
    );

    /**
     * Dynamo table hash key.
     */
    private static final String HASH = "hash-key";

    /**
     * Dynamo table range key.
     */
    private static final String RANGE = "range-key";

    /**
     * Region.Simple can work with AWS.
     * @throws Exception If some problem inside
     */
    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public void worksWithAmazon() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final Table tbl = this.region(name).table(name);
        final String attr = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final String value = RandomStringUtils.randomAlphanumeric(Tv.TEN);
        final String hash = RandomStringUtils.randomAlphanumeric(Tv.TEN);
        for (int idx = 0; idx < Tv.FIVE; ++idx) {
            tbl.put(
                new Attributes()
                    .with(RegionITCase.HASH, hash)
                    .with(RegionITCase.RANGE, idx)
                    .with(attr, value)
            );
        }
        MatcherAssert.assertThat(
            tbl.frame()
                .where(RegionITCase.HASH, Conditions.equalTo(hash))
                .through(new QueryValve().withLimit(1)),
            Matchers.hasSize(Tv.FIVE)
        );
        final Frame frame = tbl.frame()
            .where(attr, Conditions.equalTo(value))
            .through(
                new ScanValve()
                    .withLimit(Tv.TEN)
                    .withAttributeToGet(attr)
            );
        MatcherAssert.assertThat(frame, Matchers.hasSize(Tv.FIVE));
        final Iterator<Item> items = frame.iterator();
        final Item item = items.next();
        final String range = item.get(RegionITCase.RANGE).getS();
        MatcherAssert.assertThat(
            item.get(attr).getS(),
            Matchers.equalTo(value)
        );
        item.put(attr, new AttributeValue("empty"));
        MatcherAssert.assertThat(
            tbl.frame()
                .where(RegionITCase.HASH, hash)
                .where(RegionITCase.RANGE, range)
                .through(new ScanValve())
                .iterator().next()
                .get(attr).getS(),
            Matchers.not(Matchers.equalTo(value))
        );
        items.remove();
    }

    /**
     * Region.Simple can retrieve attributes.
     * @throws Exception If some problem inside
     */
    @Test
    public void retrievesAttributesFromDynamo() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final Table tbl = this.region(name).table(name);
        final String idx = "2f7whf";
        final String hash = "7afe5efa";
        final String attr = "some-attribute";
        tbl.put(
            new Attributes()
                .with(RegionITCase.HASH, hash)
                .with(RegionITCase.RANGE, idx)
                .with(attr, "test-value")
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .where(RegionITCase.HASH, hash)
                .where(RegionITCase.RANGE, idx)
                .through(new QueryValve().withAttributeToGet(attr))
                .iterator().next()
                .has("something"),
            Matchers.is(false)
        );
    }

    /**
     * Get region with a table.
     * @param table Table name
     * @return Region
     * @throws Exception If fails
     */
    private Region region(final String table) throws Exception {
        final Region region = new Region.Simple(
            new Credentials.Direct(Credentials.TEST, RegionITCase.PORT)
        );
        final TableMocker mocker = new TableMocker(
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
                        .withAttributeName(RegionITCase.HASH)
                        .withAttributeType(ScalarAttributeType.S),
                    new AttributeDefinition()
                        .withAttributeName(RegionITCase.RANGE)
                        .withAttributeType(ScalarAttributeType.S)
                )
                .withKeySchema(
                    new KeySchemaElement()
                        .withAttributeName(RegionITCase.HASH)
                        .withKeyType(KeyType.HASH),
                    new KeySchemaElement()
                        .withAttributeName(RegionITCase.RANGE)
                        .withKeyType(KeyType.RANGE)
                )
        );
        mocker.create();
        mocker.createIfAbsent();
        return region;
    }

}
