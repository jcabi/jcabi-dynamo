/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.aspects.Immutable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * One column of a table.
 * @since 0.10
 */
@Immutable
@ToString
@EqualsAndHashCode(of = "name")
final class Column {

    /**
     * Name of the column.
     */
    private final transient String name;

    /**
     * Public ctor.
     * @param key Name of the column
     */
    Column(final String key) {
        this.name = key;
    }

    /**
     * Match the column against a value.
     * @return SQL fragment
     */
    String where() {
        return String.format("`%s` = ?", this.name);
    }

    /**
     * Declare the column as a primary key.
     * @return SQL fragment
     */
    String key() {
        return String.format("`%s` VARCHAR NOT NULL", this.name);
    }

    /**
     * Declare the column as an attribute.
     * @return SQL fragment
     */
    String attribute() {
        return String.format("`%s` CLOB", this.name);
    }

    /**
     * Quote the name of the column.
     * @return SQL fragment
     */
    String quoted() {
        return String.format("`%s`", this.name);
    }
}
