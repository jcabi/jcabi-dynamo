/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
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
final class AwsIteratorTest {

    @Test
    void iteratesValve() throws IOException {
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

    @Test
    void throwsOnEmptyIterator() throws Exception {
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
