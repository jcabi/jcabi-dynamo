/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link Region}.
 * @since 0.1
 */
final class RegionTest {

    @Test
    void appendsPrefixesToTableNames() {
        final Table table = Mockito.mock(Table.class);
        final Region region = Mockito.mock(Region.class);
        Mockito.doReturn(table).when(region).table(Mockito.anyString());
        new Region.Prefixed(region, "foo-").table("test");
        Mockito.verify(region).table("foo-test");
    }

}
