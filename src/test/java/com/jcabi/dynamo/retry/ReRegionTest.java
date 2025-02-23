/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.amazonaws.AmazonClientException;
import com.jcabi.aspects.Tv;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link ReRegion}.
 * @since 0.1
 */
final class ReRegionTest {

    @Test
    void retriesAwsCalls() throws Exception {
        final Table table = Mockito.mock(Table.class);
        final Attributes attrs = new Attributes();
        final String msg = "hey you";
        Mockito.doThrow(new AmazonClientException(msg))
            .when(table).put(attrs);
        final Region origin = Mockito.mock(Region.class);
        Mockito.doReturn(table).when(origin).table(Mockito.anyString());
        final Region region = new ReRegion(origin);
        try {
            region.table("test").put(attrs);
            Assertions.fail("exception expected here");
        } catch (final AmazonClientException ex) {
            assert ex.getMessage().equals(msg);
        }
        Mockito.verify(table, Mockito.times(Tv.THREE)).put(attrs);
    }

}
