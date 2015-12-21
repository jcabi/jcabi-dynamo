/**
 * Copyright (c) 2012-2015, jcabi.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 1) Redistributions of source code must retain the above
 * copyright notice, this list of conditions and the following
 * disclaimer. 2) Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided
 * with the distribution. 3) Neither the name of the jcabi.com nor
 * the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.jcabi.dynamo.mock;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.codec.binary.Base32;
import org.h2.Driver;

/**
 * Mock data in H2 database.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.10
 * @checkstyle ClassDataAbstractionCouplingCheck (500 lines)
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
                final Collection<Attributes> items =
                    new LinkedList<Attributes>();
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
                    attrs = attrs.with(
                        meta.getColumnName(idx + 1),
                        rset.getString(idx + 1)
                    );
                }
                return attrs;
            }
        };

    /**
     * Where clause.
     */
    private static final Function<String, String> WHERE =
        new Function<String, String>() {
            @Override
            public String apply(final String key) {
                return String.format("%s = ?", key);
            }
        };

    /**
     * Create primary key.
     */
    private static final Function<String, String> CREATE_KEY =
        new Function<String, String>() {
            @Override
            public String apply(final String key) {
                return String.format("%s VARCHAR PRIMARY KEY", key);
            }
        };

    /**
     * Create attr.
     */
    private static final Function<String, String> CREATE_ATTR =
        new Function<String, String>() {
            @Override
            public String apply(final String key) {
                return String.format("%s CLOB", key);
            }
        };

    /**
     * WHERE clauses are joined with this.
     */
    private static final String AND = " AND ";

    /**
     * JDBC URL.
     */
    private final transient String jdbc;

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
        this.jdbc = String.format(
            "jdbc:h2:file:%s",
            file.getAbsolutePath()
        );
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
                    Iterables.transform(conds.keySet(), H2Data.WHERE)
                );
            }
            JdbcSession session = new JdbcSession(this.connection())
                .sql(sql.toString());
            for (final Condition cond : conds.values()) {
                if (cond.getAttributeValueList().size() != 1) {
                    throw new UnsupportedOperationException(
                        "at the moment only one value of condition is supported"
                    );
                }
                if (!cond.getComparisonOperator()
                    .equals(ComparisonOperator.EQ.toString())) {
                    throw new UnsupportedOperationException(
                        String.format(
                            "at the moment only EQ operator is supported: %s",
                            cond.getComparisonOperator()
                        )
                    );
                }
                final AttributeValue val = cond.getAttributeValueList().get(0);
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
            JdbcSession session = new JdbcSession(this.connection());
            for (final AttributeValue value : attrs.values()) {
                session = session.set(H2Data.value(value));
            }
            session.sql(
                String.format(
                    "INSERT INTO %s (%s) VALUES (%s)",
                    H2Data.encodeTableName(table),
                    Joiner.on(',').join(attrs.keySet()),
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
            JdbcSession session = new JdbcSession(this.connection());
            for (final AttributeValueUpdate value : attrs.values()) {
                session = session.set(H2Data.value(value.getValue()));
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
            JdbcSession session = new JdbcSession(this.connection());
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
                String.format("empty list of keys for %s table", table)
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
            new JdbcSession(this.connection()).sql(sql.toString()).execute();
        } catch (final SQLException ex) {
            throw new IOException(ex);
        }
        return this;
    }
    /**
     * Make data source.
     * @return Data source for JDBC
     * @throws SQLException If fails
     */
    private Connection connection() throws SQLException {
        return new Driver().connect(this.jdbc, new Properties());
    }

    /**
     * Get value from attribute.
     * @param attr Attribute value
     * @return Text format
     */
    private static String value(final AttributeValue attr) {
        String val = attr.getS();
        if (val == null) {
            val = attr.getN();
        }
        if (val == null) {
            throw new IllegalArgumentException(
                "we support only N and S at the moment"
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
