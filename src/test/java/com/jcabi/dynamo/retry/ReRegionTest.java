/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Test case for {@link ReRegion}.
 * @since 0.1
 */
final class ReRegionTest {

    @Test
    void retriesAwsCalls() throws Exception {
        final Table table = Mockito.mock(Table.class);
        Mockito.doThrow(
            SdkClientException.builder().message("hey you").build()
        ).when(table).put(new Attributes());
        final Region origin = Mockito.mock(Region.class);
        Mockito.doReturn(table).when(origin).table(Mockito.anyString());
        final Table retried = new ReRegion(origin).table("test");
        Assertions.assertThrows(
            SdkClientException.class,
            () -> retried.put(new Attributes())
        );
    }

}
