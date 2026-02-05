/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Table;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link ReTable}.
 * @since 0.1
 */
final class ReTableTest {

    @Test
    void retriesDeleteOnFailure() throws Exception {
        final Table table = Mockito.mock(Table.class);
        Mockito.doThrow(new IOException("Exception!")).when(table)
            .delete(new Attributes());
        final Table retried = new ReTable(table);
        Assertions.assertThrows(
            IOException.class,
            () -> retried.delete(new Attributes())
        );
    }
}
