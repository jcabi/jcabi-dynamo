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
import com.google.common.collect.ImmutableMap;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.ArrayMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Row in CSV data.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.10
 */
@Immutable
public interface MkRow {

    /**
     * Get immutable map of attributes.
     * @return Attrs
     */
    Map<String, AttributeValue> attributes();

    /**
     * Put this attribute.
     * @param name Name
     * @param value Value
     */
    void put(String name, String value);

    /**
     * Simple.
     */
    @Immutable
    @ToString
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = { "tbl", "map" })
    final class Simple implements MkRow {
        /**
         * Table name.
         */
        private final transient String tbl;
        /**
         * Map of attributes.
         */
        private final transient ArrayMap<String, String> map;
        /**
         * Public ctor.
         * @param table Table name
         */
        public Simple(final String table, final Map<String, String> attrs) {
            this.tbl = table;
            this.map = new ArrayMap<String, String>(attrs);
        }
        @Override
        public String table() {
            return this.tbl;
        }
        @Override
        public Map<String, String> attrs() {
            return this.map;
        }
    }

    /**
     * In CSV file.
     */
    @Immutable
    @ToString
    @Loggable(Loggable.DEBUG)
    @EqualsAndHashCode(of = "row")
    final class CSV implements MkRow {
        /**
         * Row.
         */
        private final transient MkRow row;
        /**
         * Public ctor.
         * @param origin Origin row
         */
        public CSV(final MkRow origin) {
            this.row = origin;
        }
        /**
         * Public ctor.
         * @param text Line with text
         */
        public CSV(final String text) {
            final String[] parts = text.split(",");
            final ImmutableMap.Builder<String, String> map =
                new ImmutableMap.Builder<String, String>();
            for (int idx = 1; idx < parts.length; ++idx) {
                final String[] pair = parts[idx].split(":");
                map.put(pair[0], pair[1]);
            }
            this.row = new MkRow.Simple(parts[0], map.build());
        }
        @Override
        public String toString() {
            final StringBuilder out = new StringBuilder(this.table());
            for (final Map.Entry<String, String> entry
                : this.attrs().entrySet()) {
                out.append(',')
                    .append(entry.getKey())
                    .append(':')
                    .append(entry.getValue());
            }
            return out.append('\n').toString();
        }
        @Override
        public String table() {
            return this.row.table();
        }
        @Override
        public Map<String, String> attrs() {
            return this.row.attrs();
        }
    }
}
