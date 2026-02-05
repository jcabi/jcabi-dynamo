/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

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
    void hasNextOnFreshIterator() throws IOException {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final Dosage first = Mockito.mock(Dosage.class);
        Mockito.doReturn(
            Collections.singletonList(
                new Attributes().with("attribute-1", "value-1")
            )
        ).when(first).items();
        Mockito.doReturn(true).when(first).hasNext();
        final Valve valve = Mockito.mock(Valve.class);
        Mockito.doReturn(first)
            .when(valve)
            .fetch(
                Mockito.eq(credentials), Mockito.anyString(),
                Mockito.any(Map.class), Mockito.any(Collection.class)
            );
        final String table = "table-1";
        MatcherAssert.assertThat(
            "should has next",
            new AwsIterator(
                credentials,
                new AwsFrame(
                    credentials,
                    new AwsTable(
                        credentials,
                        Mockito.mock(Region.class),
                        table
                    ),
                    table
                ),
                table, new Conditions(),
                new ArrayList<>(0), valve
            ).hasNext(),
            Matchers.is(true)
        );
    }

    @Test
    void returnsFirstItemValue() throws IOException {
        final Credentials credentials = Mockito.mock(Credentials.class);
        final String attr = "attribute-1";
        final String value = "value-1";
        final Dosage first = Mockito.mock(Dosage.class);
        Mockito.doReturn(
            Collections.singletonList(new Attributes().with(attr, value))
        ).when(first).items();
        Mockito.doReturn(true).when(first).hasNext();
        final Valve valve = Mockito.mock(Valve.class);
        Mockito.doReturn(first)
            .when(valve)
            .fetch(
                Mockito.eq(credentials), Mockito.anyString(),
                Mockito.any(Map.class), Mockito.any(Collection.class)
            );
        final String table = "table-1";
        MatcherAssert.assertThat(
            "should equal to 'value-1'",
            new AwsIterator(
                credentials,
                new AwsFrame(
                    credentials,
                    new AwsTable(
                        credentials,
                        Mockito.mock(Region.class),
                        table
                    ),
                    table
                ),
                table, new Conditions(),
                new ArrayList<>(0), valve
            ).next().get(attr).s(),
            Matchers.equalTo(value)
        );
    }

    @Test
    void hasNextAfterFirstDosageConsumed() throws IOException {
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
        Mockito.doReturn(false).when(second).hasNext();
        final Valve valve = Mockito.mock(Valve.class);
        Mockito.doReturn(first)
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
                new AwsTable(
                    credentials,
                    Mockito.mock(Region.class),
                    table
                ),
                table
            ),
            table, new Conditions(),
            new ArrayList<>(0), valve
        );
        iterator.next();
        MatcherAssert.assertThat(
            "should has next",
            iterator.hasNext(),
            Matchers.is(true)
        );
    }

    @Test
    void hasNoNextAfterAllConsumed() throws IOException {
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
                new AwsTable(
                    credentials,
                    Mockito.mock(Region.class),
                    table
                ),
                table
            ),
            table, new Conditions(),
            new ArrayList<>(0), valve
        );
        iterator.next();
        iterator.next();
        MatcherAssert.assertThat(
            "should not has next",
            iterator.hasNext(),
            Matchers.is(false)
        );
    }

    @Test
    void throwsOnEmptyIterator() throws Exception {
        final Credentials creds = Mockito.mock(Credentials.class);
        final Dosage dosage = Mockito.mock(Dosage.class);
        Mockito.doReturn(Collections.emptyList()).when(dosage).items();
        final Valve vlv = Mockito.mock(Valve.class);
        Mockito.doReturn(dosage)
            .when(vlv)
            .fetch(
                Mockito.eq(creds), Mockito.anyString(),
                Mockito.any(Map.class), Mockito.any(Collection.class)
            );
        final String table = "table-2";
        Assertions.assertThrows(
            NoSuchElementException.class,
            new AwsIterator(
                creds,
                new AwsFrame(
                    creds,
                    new AwsTable(
                        creds,
                        Mockito.mock(Region.class),
                        table
                    ),
                    table
                ),
                table, new Conditions(),
                new ArrayList<>(0), vlv
            )::next
        );
    }

}
