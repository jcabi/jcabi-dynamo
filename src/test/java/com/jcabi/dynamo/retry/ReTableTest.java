/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.retry;

import com.jcabi.aspects.Tv;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Table;
import java.io.IOException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link ReTable}.
 * @since 0.1
 */
final class ReTableTest {

    @Test
    void retriesDelete() throws Exception {
        final Table table = Mockito.mock(Table.class);
        final Attributes attrs = new Attributes();
        final String msg = "Exception!";
        Mockito.doThrow(new IOException(msg)).when(table)
            .delete(attrs);
        final Table retry = new ReTable(table);
        try {
            retry.delete(attrs);
        } catch (final IOException ex) {
            MatcherAssert.assertThat(ex.getMessage(), Matchers.equalTo(msg));
        }
        Mockito.verify(table, Mockito.times(Tv.THREE)).delete(attrs);
    }
}
