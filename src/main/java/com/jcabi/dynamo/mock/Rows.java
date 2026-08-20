/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.aspects.Immutable;
import com.jcabi.dynamo.Attributes;
import com.jcabi.jdbc.Outcome;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * All rows of a result set.
 * @since 0.10
 */
@Immutable
@ToString
@EqualsAndHashCode
final class Rows implements Outcome<Iterable<Attributes>> {

    @Override
    public Iterable<Attributes> handle(final ResultSet rset,
        final Statement stmt) throws SQLException {
        final Collection<Attributes> items = new ArrayList<>(0);
        while (rset.next()) {
            items.add(Rows.fetch(rset));
        }
        return items;
    }

    private static Attributes fetch(final ResultSet rset) throws SQLException {
        final ResultSetMetaData meta = rset.getMetaData();
        Attributes attrs = new Attributes();
        for (int idx = 0; idx < meta.getColumnCount(); ++idx) {
            final String text = rset.getString(idx + 1);
            final AttributeValue value;
            if (text.matches("[0-9]+")) {
                value = AttributeValue.builder().s(text).n(text).build();
            } else {
                value = AttributeValue.builder().s(text).build();
            }
            attrs = attrs.with(
                meta.getColumnName(idx + 1).toLowerCase(Locale.ENGLISH),
                value
            );
        }
        return attrs;
    }
}
