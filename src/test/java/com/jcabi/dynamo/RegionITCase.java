/**
 * Copyright (c) 2012-2017, jcabi.com
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

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.jcabi.aspects.Tv;
import java.util.Iterator;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration case for {@link Region}.
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.1
 */
public final class RegionITCase {

    @BeforeEach
    public void itTestCheck() {
        Assumptions.assumeFalse(System.getProperty("failsafe.port", "").isEmpty());
    }

    @Test
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public void worksWithAmazon() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final String attr = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final String value = RandomStringUtils.randomAlphanumeric(Tv.TEN);
        final String hash = RandomStringUtils.randomAlphanumeric(Tv.TEN);
        for (int idx = 0; idx < Tv.FIVE; ++idx) {
            tbl.put(
                new Attributes()
                    .with(mock.hash(), hash)
                    .with(mock.range(), idx)
                    .with(attr, value)
            );
        }
        MatcherAssert.assertThat(
            tbl.frame()
                .where(mock.hash(), Conditions.equalTo(hash))
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
        final int range = Integer.parseInt(item.get(mock.range()).getN());
        MatcherAssert.assertThat(
            item.get(attr).getS(),
            Matchers.equalTo(value)
        );
        item.put(
            attr,
            new AttributeValueUpdate(
                new AttributeValue("empty"),
                AttributeAction.PUT
            )
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .where(mock.hash(), hash)
                .where(mock.range(), Conditions.equalTo(range))
                .through(new ScanValve())
                .iterator().next()
                .get(attr).getS(),
            Matchers.not(Matchers.equalTo(value))
        );
        items.remove();
    }

    @Test
    @Disabled
    public void retrievesAttributesFromDynamo() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(Tv.EIGHT);
        final RegionMock mock = new RegionMock();
        final Table tbl = mock.get(name).table(name);
        final int idx = Tv.TEN;
        final String hash = "7afe5efa";
        final String attr = "some-attribute";
        tbl.put(
            new Attributes()
                .with(mock.hash(), hash)
                .with(mock.range(), idx)
                .with(attr, "test-value")
        );
        MatcherAssert.assertThat(
            tbl.frame()
                .where(mock.hash(), hash)
                .where(mock.range(), Conditions.equalTo(idx))
                .through(
                    new QueryValve()
                        .withAttributeToGet(attr)
                        .withConsistentRead(true)
                        .withLimit(Tv.FIFTY)
                )
                .iterator().next()
                .has("something"),
            Matchers.is(false)
        );
    }

}
