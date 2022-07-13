/**
 * Copyright (c) 2012-2022, jcabi.com
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
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test case for {@link MkRegion}.
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.10
 * @checkstyle MultipleStringLiteralsCheck (500 lines)
 */
public final class MkRegionTest {

    /**
     * MkRegion can store and read items.
     * @throws Exception If some problem inside
     */
    @Test
    public void storesAndReadsAttributes() throws Exception {
        final String name = "users";
        final String key = "id";
        final String attr = "description";
        final String nattr = "thenumber";
        final Region region = new MkRegion(
            new H2Data().with(name, new String[] {key}, attr, nattr)
        );
        final Table table = region.table(name);
        table.put(
            new Attributes()
                .with(key, "32443")
                .with(attr, "first value to \n\t€ save")
                .with(nattr, "150")
        );
        final Item item = table.frame().iterator().next();
        MatcherAssert.assertThat(item.has(attr), Matchers.is(true));
        MatcherAssert.assertThat(
            item.get(attr).getS(),
            Matchers.containsString("\n\t\u20ac save")
        );
        item.put(
            attr,
            new AttributeValueUpdate().withValue(
                new AttributeValue("this is another value")
            )
        );
        MatcherAssert.assertThat(
            item.get(attr).getS(),
            Matchers.containsString("another value")
        );
        MatcherAssert.assertThat(
            item.get(nattr).getN(),
            Matchers.endsWith("50")
        );
    }

    /**
     * MkRegion can store and read items.
     * @throws Exception If some problem inside
     */
    @Test
    public void storesAndReadsSingleAttribute() throws Exception {
        final String table = "ideas";
        final String key = "number";
        final String attr = "total";
        final Region region = new MkRegion(
            new H2Data().with(table, new String[] {key}, attr)
        );
        final Table tbl = region.table(table);
        tbl.put(
            new Attributes()
                .with(key, "32443")
                .with(attr, "0")
        );
        final Item item = tbl.frame().iterator().next();
        item.put(
            attr,
            new AttributeValueUpdate().withValue(
                new AttributeValue().withN("2")
            ).withAction(AttributeAction.PUT)
        );
        MatcherAssert.assertThat(item.get(attr).getN(), Matchers.equalTo("2"));
    }

}
