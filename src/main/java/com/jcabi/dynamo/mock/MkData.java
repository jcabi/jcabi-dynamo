/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import com.jcabi.aspects.Immutable;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import java.io.IOException;

/**
 * Mock data.
 *
 * @since 0.10
 */
@Immutable
public interface MkData {

    /**
     * Get keys for the given table.
     * @param table Name of the table
     * @return All keys of the table
     * @throws IOException If fails
     */
    Iterable<String> keys(String table) throws IOException;

    /**
     * Iterate everything for the given table.
     * @param table Name of the table
     * @param conds Conditions
     * @return All rows found
     * @throws IOException If fails
     */
    Iterable<Attributes> iterate(String table, Conditions conds)
        throws IOException;

    /**
     * Add new attribute into the given table.
     * @param table Table name
     * @param attrs Attributes to save
     * @throws IOException If fails
     */
    void put(String table, Attributes attrs) throws IOException;

    /**
     * Add new attribute into the given table.
     * @param table Table name
     * @param keys Keys
     * @param attrs Attributes to save
     * @throws IOException If fails
     */
    void update(String table, Attributes keys,
        AttributeUpdates attrs) throws IOException;

    /**
     * Delete attributes from the given table.
     * @param table Table name
     * @param keys Keys
     * @throws IOException If fails
     */
    void delete(String table, Attributes keys) throws IOException;
}
