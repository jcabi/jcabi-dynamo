/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

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
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.codec.binary.Base32;
import org.h2.jdbcx.JdbcDataSource;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * Mock data in H2 database.
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = "jdbc")
public final class H2Data implements MkData {

    /**
     * Fetcher of rows.
     */
    private static final Outcome<Iterable<Attributes>> OUTCOME =
        new Rows();

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
        this(
            H2Data.connection(
                String.format(
                    "jdbc:h2:file:%s",
                    file.getAbsolutePath()
                )
            )
        );
    }

    /**
     * Private ctor.
     * @param source Data source of the database
     */
    private H2Data(final DataSource source) {
        this.jdbc = source;
    }

    @Override
    public Iterable<String> keys(final String table) throws IOException {
        try {
            return new JdbcSession(this.jdbc).sql(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ?"
            ).set(H2Data.encodeTableName(table)).select(
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
                    Iterables.transform(
                        conds.entrySet(),
                        cnd -> new Clause(cnd).sql()
                    )
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
                session = session.set(H2Data.value(cond.attributeValueList().get(0)));
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
                        Iterables.transform(
                            attrs.keySet(),
                            key -> new Column(key).where()
                        )
                    ),
                    Joiner.on(H2Data.AND).join(
                        Iterables.transform(
                            keys.keySet(),
                            key -> new Column(key).where()
                        )
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
                        Iterables.transform(
                            keys.keySet(),
                            key -> new Column(key).where()
                        )
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
        final StringBuilder sql = new StringBuilder(128)
            .append("CREATE TABLE ")
            .append(H2Data.encodeTableName(table)).append(" (");
        Joiner.on(',').appendTo(
            sql,
            Iterables.transform(
                Arrays.asList(keys),
                key -> new Column(key).key()
            )
        );
        if (attrs.length > 0) {
            sql.append(',');
            Joiner.on(',').appendTo(
                sql,
                Iterables.transform(
                    Arrays.asList(attrs),
                    key -> new Column(key).attribute()
                )
            );
        }
        sql.append(", PRIMARY KEY (");
        Joiner.on(',').appendTo(
            sql,
            Iterables.transform(
                Arrays.asList(keys),
                key -> new Column(key).quoted()
            )
        );
        sql.append("))");
        try {
            new JdbcSession(this.jdbc).sql(sql.toString()).execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
        return this;
    }

    private static DataSource connection(final String jdbc) {
        final JdbcDataSource src = new JdbcDataSource();
        src.setURL(jdbc);
        return src;
    }

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

    private static String encodeTableName(final String table) {
        return Joiner.on("").join(
            "_",
            Base32.builder()
                .setHexEncodeTable(true)
                .setPadding((byte) '_')
                .get()
                .encodeAsString(table.getBytes(StandardCharsets.UTF_8))
        );
    }
}
