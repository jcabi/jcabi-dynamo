/*
 * Copyright (c) 2012-2022, jcabi.com
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
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.dynamo.Attributes;
import com.jcabi.dynamo.Frame;
import com.jcabi.dynamo.Item;
import com.jcabi.dynamo.Region;
import com.jcabi.dynamo.Table;
import java.io.IOException;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Mock version of {@link Table}.
 *
 * @since 0.10
 */
@Immutable
@ToString
@Loggable(Loggable.DEBUG)
@EqualsAndHashCode(of = { "self", "data" })
final class MkTable implements Table {

    /**
     * Data.
     */
    private final transient MkData data;

    /**
     * Name of the table.
     */
    private final transient String self;

    /**
     * Public ctor.
     * @param dta Data
     * @param name Name of the table
     */
    MkTable(final MkData dta, final String name) {
        this.data = dta;
        this.self = name;
    }

    @Override
    public Item put(final Map<String, AttributeValue> attributes) {
        final Attributes attrs = new Attributes(attributes);
        try {
            this.data.put(this.self, attrs);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
        return new MkItem(this.data, this.self, attrs);
    }

    @Override
    public Frame frame() {
        return new MkFrame(this.data, this.self);
    }

    @Override
    public Region region() {
        return new MkRegion(this.data);
    }

    @Override
    public String name() {
        return this.self;
    }

    @Override
    public void delete(final Map<String, AttributeValue> attributes)
        throws IOException {
        this.data.delete(
            this.self,
            new Attributes(attributes)
        );
    }
}
