/**
 * Copyright (c) 2012-2013, JCabi.com
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
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
     * Iterate everything for the given table.
     * @param table Name of the table
     * @param conds Conditions
     * @return All rows found
     */
    Iterable<Map<String, AttributeValue>> iterate(String table,
        Map<String, Condition> conds);

    /**
     * Add new attribute into the given table.
     * @param table Table name
     * @param name Name of attribute
     * @param value Value to set
     */
    void put(String table, String name, AttributeValue value);

    @Immutable
    @ToString
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = "path")
    final class InFile implements MkData {
        /**
         * File name.
         */
        private final transient String path;
        /**
         * Public ctor.
         * @throws IOException If fails
         */
        public InFile() throws IOException {
            this(File.createTempFile("jcabi-dynamo-", ".csv"));
        }
        /**
         * Public ctor.
         */
        public InFile(final File file) {
            this.path = file.getAbsolutePath();
        }
        @Override
        public Iterable<MkRow> rows() {
            final Collection<String> lines;
            try {
                lines = Files.readLines(new File(this.path), Charsets.UTF_8);
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
            return Iterables.transform(
                lines,
                new Function<String, MkRow>() {
                    @Override
                    public MkRow apply(final String input) {
                        return new MkRow.CSV(input);
                    }
                }
            );
        }
        @Override
        public MkRow add() {
            try {
                Files.append(
                    new MkRow.CSV(row).toString(),
                    new File(this.path), Charsets.UTF_8
                );
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }
}
