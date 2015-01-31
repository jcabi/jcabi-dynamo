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
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Joiner;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import java.io.File;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test case for {@link H2Data}.
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.10
 */
public final class H2DataTest {

    /**
     * Temp directory.
     * @checkstyle VisibilityModifierCheck (5 lines)
     */
    @Rule
    public final transient TemporaryFolder temp = new TemporaryFolder();

    /**
     * H2Data can store and fetch.
     * @throws Exception If some problem inside
     */
    @Test
    public void storesAndReadsAttributes() throws Exception {
        final String table = "users";
        final String key = "id";
        final int number = 43;
        final String attr = "description";
        final String value = "some\n\t\u20ac text";
        final MkData data = new H2Data().with(
            table, new String[] {key}, new String[] {attr}
        );
        data.put(table, new Attributes().with(key, number).with(attr, value));
        MatcherAssert.assertThat(
            data.iterate(
                table, new Conditions().with(key, Conditions.equalTo(number))
            ).iterator().next(),
            Matchers.hasEntry(
                Matchers.equalTo(attr),
                Matchers.equalTo(new AttributeValue(value))
            )
        );
    }

    /**
     * H2Data can store to a file.
     * @throws Exception If some problem inside
     * @see https://code.google.com/p/h2database/issues/detail?id=447
     */
    @Test
    @Ignore
    public void storesToFile() throws Exception {
        final File file = this.temp.newFile();
        final String table = "tbl";
        final String key = "key1";
        final MkData data = new H2Data(file).with(
            table, new String[] {key}, new String[0]
        );
        data.put(table, new Attributes().with(key, "x2"));
        MatcherAssert.assertThat(file.exists(), Matchers.is(true));
        MatcherAssert.assertThat(file.length(), Matchers.greaterThan(0L));
    }

    /**
     * H2Data can create many tables.
     * @throws Exception If some problem inside
     */
    @Test
    public void createsManyTables() throws Exception {
        new H2Data()
            .with("firsttable", new String[] {"firstid"}, new String[0])
            .with("secondtable", new String[]{"secondid"}, new String[0]);
    }

    /**
     * H2Data can create tables with long names (max length of DynamoDb table
     * name is 255 characters).
     * @throws Exception In case test fails
     */
    @Test
    public void createsTablesWithLongNames() throws Exception {
        new H2Data()
            .with(
                //@checkstyle MagicNumberCheck (1 line)
                Joiner.on("").join(Collections.nCopies(255, "a")),
                new String[] {"key"}, new String[0]
        );
    }

    /**
     * H2Data supports table names with characters illegal to H2.
     * @throws Exception In case test fails
     */
    @Test
    public void supportsTableNamesWithIllegalCharacters() throws Exception {
        new H2Data().with(".-.", new String[]{"pk"}, new String[0]);
    }

    /**
     * H2Data supports column names with characters illegal to H2.
     * todo: #28 user reports that H2Data does not support
     * COLUMNS with ".", "-", or digits but i don't know for sure
     * should it support these symbols or not.
     * It's needed to be confirmed and test should be uncommented
     * if H2Data must support mentioned symbols.
     * @throws Exception In case test fails
     */
    @Test
    @Ignore
    public void supportsColumnNamesWithIllegalCharacters() throws Exception {
        final String key = "0-.col.-0";
        final String table = "test";
        new H2Data().with(
            table, new String[] {key}, new String[0]
        ).put(table, new Attributes().with(key, "value"));
    }

}
