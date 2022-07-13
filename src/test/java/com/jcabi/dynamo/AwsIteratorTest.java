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
package com.jcabi.dynamo;

import com.jcabi.aspects.Tv;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link AwsIterator}.
 * @since 0.1
 */
@SuppressWarnings("unchecked")
public final class AwsIteratorTest {

    /**
     * AwsIterator can iterate using valve.
     * @throws IOException If some problem inside
     * @checkstyle ExecutableStatementCount (100 lines)
     */
    @Test
    public void iteratesValve() throws IOException {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final String attr = "attribute-1";
        final String value = "value-1";
        final Dosage first = Mockito.mock(Dosage.class);
        Mockito.doReturn(
            Collections.singletonList(new Attributes().with(attr, value))
        ).when(first).items();
        Mockito.doReturn(true).when(first).hasNext();
        final Dosage second = Mockito.mock(Dosage.class);
        Mockito.doReturn(second).when(first).next();
        Mockito.doReturn(
            Collections.singletonList(new Attributes().with(attr, value))
        ).when(second).items();
        Mockito.doReturn(true).when(second).hasNext();
        final Dosage last = Mockito.mock(Dosage.class);
        Mockito.doReturn(last).when(second).next();
        Mockito.doReturn(new ArrayList<Attributes>(0)).when(last).items();
        final Valve valve = Mockito.mock(Valve.class);
        Mockito.doReturn(first)
            .doReturn(second)
            .doReturn(last)
            .when(valve)
            .fetch(
                Mockito.eq(credentials), Mockito.anyString(),
                Mockito.any(Map.class), Mockito.any(Collection.class)
            );
        final String table = "table-1";
        final Iterator<Item> iterator = new AwsIterator(
            credentials,
            new AwsFrame(
                credentials,
                new AwsTable(credentials, Mockito.mock(Region.class), table),
                table
            ),
            table, new Conditions(),
            new ArrayList<>(0), valve
        );
        for (int idx = 0; idx < Tv.TEN; ++idx) {
            MatcherAssert.assertThat(iterator.hasNext(), Matchers.is(true));
        }
        Mockito.verify(valve).fetch(
            Mockito.eq(credentials), Mockito.anyString(),
            Mockito.any(Map.class), Mockito.any(Collection.class)
        );
        MatcherAssert.assertThat(
            iterator.next().get(attr).getS(),
            Matchers.equalTo(value)
        );
        MatcherAssert.assertThat(iterator.hasNext(), Matchers.is(true));
        Mockito.verify(first).next();
        MatcherAssert.assertThat(
            iterator.next().get(attr).getS(),
            Matchers.equalTo(value)
        );
        MatcherAssert.assertThat(iterator.hasNext(), Matchers.is(false));
        Mockito.verify(second).next();
    }

    /**
     * AwsIterator can iterate using valve.
     * @throws Exception If some problem inside
     */
    @Test
    public void throwsOnEmptyIterator() throws Exception {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final Dosage dosage = Mockito.mock(Dosage.class);
        Mockito.doReturn(Collections.emptyList()).when(dosage).items();
        final Valve valve = Mockito.mock(Valve.class);
        Mockito.doReturn(dosage)
            .when(valve)
            .fetch(
                Mockito.eq(credentials), Mockito.anyString(),
                Mockito.any(Map.class), Mockito.any(Collection.class)
            );
        final String table = "table-2";
        final Iterator<Item> iterator = new AwsIterator(
            credentials,
            new AwsFrame(
                credentials,
                new AwsTable(credentials, Mockito.mock(Region.class), table),
                table
            ),
            table, new Conditions(),
            new ArrayList<>(0), valve
        );
        Assertions.assertThrows(
            NoSuchElementException.class,
            iterator::next
        );
    }

}
