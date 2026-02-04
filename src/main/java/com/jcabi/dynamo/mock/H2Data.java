/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.ListOutcome;
import com.jcabi.jdbc.Outcome;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.codec.binary.Base32;
import org.h2.jdbcx.JdbcDataSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Mock data in H2 database.
 *
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "jdbc")
@SuppressWarnings({ "PMD.TooManyMethods", "PMD.ExcessiveImports" })
public final class H2Data implements MkData {

    /**
     * Fetcher of rows.
     */
    private static final Outcome<Iterable<Attributes>> OUTCOME =
        // @checkstyle AnonInnerLengthCheck (50 lines)
        new Outcome<Iterable<Attributes>>() {
            @Override
            public Iterable<Attributes> handle(final ResultSet rset,
                final Statement stmt) throws SQLException {
                final Collection<Attributes> items = new LinkedList<>();
                while (rset.next()) {
                    items.add(this.fetch(rset));
                }
                return items;
            }

            /**
             * Convert result set to Attributes.
             * @param rset Result set
             * @return Attribs
             * @throws SQLException If fails
             */
            private Attributes fetch(final ResultSet rset) throws SQLException {
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
        };

    /**
     * Where clause.
     */
    private static final Function<String, String> WHERE =
        key -> String.format("`%s` = ?", key);

    /**
     * Select WHERE.
     * @checkstyle AnonInnerLengthCheck (100 lines)
     * @checkstyle LineLength (3 lines)
     */
    private static final Function<Map.Entry<String, Condition>, String> SELECT_WHERE = cnd -> {
        final String opr;
        if (cnd.getValue().comparisonOperatorAsString()
            .equals(ComparisonOperator.GT.toString())) {
            opr = ">";
        } else if (cnd.getValue().comparisonOperatorAsString()
            .equals(ComparisonOperator.LT.toString())) {
            opr = "<";
        } else if (cnd.getValue().comparisonOperatorAsString()
            .equals(ComparisonOperator.EQ.toString())) {
            opr = "=";
        } else {
            throw new UnsupportedOperationException(
                String.format(
                    // @checkstyle LineLength (1 line)
                    "At the moment only EQ/GT/LT operators are supported: %s",
                    cnd.getValue().comparisonOperatorAsString()
                )
            );
        }
        return String.format(
            "`%s` %s ?", cnd.getKey(), opr
        );
    };

    /**
     * Create primary key.
     */
    private static final Function<String, String> CREATE_KEY =
        key -> String.format("`%s` VARCHAR PRIMARY KEY", key);

    /**
     * Create attr.
     */
    private static final Function<String, String> CREATE_ATTR =
        key -> String.format("`%s` CLOB", key);

    /**
     * WHERE clauses are joined with this.
     */
    private static final String AND = " AND ";

    /**
     * JDBC data source.
     */
    private final transient DataSource jdbc;

    /**
     * Public ctor.
     * @throws IOException If fails
     */
    public H2Data() throws IOException {
        this(File.createTempFile("jcabi-dynamo-", ".h2"));
    }

    /**
     * Public ctor.
     * @param file Where to keep the database
     */
    public H2Data(final File file) {
        this.jdbc = H2Data.connection(
            String.format(
                "jdbc:h2:file:%s",
                file.getAbsolutePath()
            )
        );
    }

    @Override
    public Iterable<String> keys(final String table) throws IOException {
        try {
            return new JdbcSession(this.jdbc)
                // @checkstyle LineLength (1 line)
                .sql(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ?"
                ).set(H2Data.encodeTableName(table))
                .select(
                    new ListOutcome<>(
                        rset -> rset.getString(1).toLowerCase(Locale.ENGLISH)
                    )
                );
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Iterable<Attributes> iterate(final String table,
        final Conditions conds) throws IOException {
        try {
            final StringBuilder sql = new StringBuilder("SELECT * FROM ")
                .append(H2Data.encodeTableName(table));
            if (!conds.isEmpty()) {
                sql.append(" WHERE ");
                Joiner.on(H2Data.AND).appendTo(
                    sql,
                    Iterables.transform(conds.entrySet(), H2Data.SELECT_WHERE)
                );
            }
            JdbcSession session = new JdbcSession(this.jdbc)
                .sql(sql.toString());
            for (final Condition cond : conds.values()) {
                if (cond.attributeValueList().size() != 1) {
                    throw new UnsupportedOperationException(
                        "At the moment only one value of condition is supported"
                    );
                }
                final AttributeValue val = cond.attributeValueList().get(0);
                session = session.set(H2Data.value(val));
            }
            return session.select(H2Data.OUTCOME);
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void put(final String table, final Attributes attrs)
        throws IOException {
        try {
            JdbcSession session = new JdbcSession(this.jdbc);
            for (final AttributeValue value : attrs.values()) {
                session = session.set(H2Data.value(value));
            }
            session.sql(
                String.format(
                    "INSERT INTO %s (%s) VALUES (%s)",
                    H2Data.encodeTableName(table),
                    attrs.keySet()
                        .stream()
                        .map(a -> String.format("`%s`", a))
                        .collect(Collectors.joining(", ")),
                    Joiner.on(',').join(Collections.nCopies(attrs.size(), "?"))
                )
            );
            session.execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void update(final String table, final Attributes keys,
        final AttributeUpdates attrs)
        throws IOException {
        try {
            JdbcSession session = new JdbcSession(this.jdbc);
            for (final AttributeValueUpdate value : attrs.values()) {
                session = session.set(H2Data.value(value.value()));
            }
            for (final AttributeValue value : keys.values()) {
                session = session.set(H2Data.value(value));
            }
            session.sql(
                String.format(
                    "UPDATE %s SET %s WHERE %s",
                    H2Data.encodeTableName(table),
                    Joiner.on(',').join(
                        Iterables.transform(attrs.keySet(), H2Data.WHERE)
                    ),
                    Joiner.on(H2Data.AND).join(
                        Iterables.transform(keys.keySet(), H2Data.WHERE)
                    )
                )
            );
            session.execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void delete(final String table, final Attributes keys)
        throws IOException {
        try {
            JdbcSession session = new JdbcSession(this.jdbc);
            for (final AttributeValue value : keys.values()) {
                session = session.set(H2Data.value(value));
            }
            session.sql(
                String.format(
                    "DELETE FROM %s WHERE %s",
                    H2Data.encodeTableName(table),
                    Joiner.on(H2Data.AND).join(
                        Iterables.transform(keys.keySet(), H2Data.WHERE)
                    )
                )
            );
            session.execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * With this table, that has given primary keys.
     * @param table Table name
     * @param keys Primary keys
     * @param attrs Attributes
     * @return New data, modified
     * @throws IOException If fails
     */
    public H2Data with(final String table, final String[] keys,
        final String... attrs) throws IOException {
        if (keys.length == 0) {
            throw new IllegalArgumentException(
                String.format("Empty list of keys for %s table", table)
            );
        }
        final StringBuilder sql = new StringBuilder("CREATE TABLE ")
            .append(H2Data.encodeTableName(table)).append(" (");
        Joiner.on(',').appendTo(
            sql,
            Iterables.transform(Arrays.asList(keys), H2Data.CREATE_KEY)
        );
        if (attrs.length > 0) {
            sql.append(',');
            Joiner.on(',').appendTo(
                sql,
                Iterables.transform(Arrays.asList(attrs), H2Data.CREATE_ATTR)
            );
        }
        sql.append(')');
        try {
            new JdbcSession(this.jdbc).sql(sql.toString()).execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
        return this;
    }

    /**
     * Make data source.
     * @param jdbc URL
     * @return Data source for JDBC
     */
    private static DataSource connection(final String jdbc) {
        final JdbcDataSource src = new JdbcDataSource();
        src.setURL(jdbc);
        return src;
    }

    /**
     * Get value from attribute.
     * @param attr Attribute value
     * @return Text format
     */
    private static String value(final AttributeValue attr) {
        String val = attr.s();
        if (val == null) {
            val = attr.n();
        }
        if (val == null) {
            throw new IllegalArgumentException(
                "We support only N and S at the moment"
            );
        }
        return val;
    }

    /**
     * Base32-encodes table name for use with H2.
     * @param table Table name to encode
     * @return Base-32-encoded table name
     */
    private static String encodeTableName(final String table) {
        return Joiner.on("").join(
            "_",
            new Base32(true, (byte) '_').encodeAsString(table.getBytes())
        );
    }

}
