/**
 * Copyright (c) 2012-2017, jcabi.com
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

import com.jcabi.aspects.Immutable;
import com.jcabi.dynamo.AttributeUpdates;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Conditions;
import java.io.IOException;

/**
 * Mock data.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
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
