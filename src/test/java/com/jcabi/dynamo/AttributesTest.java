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
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.jcabi.immutable.ArrayMap;
import java.util.Collections;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Test case for {@link Attributes}.
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.1
 */
@SuppressWarnings("PMD.UseConcurrentHashMap")
public final class AttributesTest {

    /**
     * Attributes can work as a map of attributes.
     * @throws Exception If some problem inside
     */
    @Test
    public void workAsMapOfAttributes() throws Exception {
        final String attr = "id";
        final AttributeValue value = new AttributeValue("some text value");
        final Map<String, AttributeValue> attrs = new Attributes()
            .with(attr, value);
        MatcherAssert.assertThat(attrs.keySet(), Matchers.hasSize(1));
        MatcherAssert.assertThat(attrs, Matchers.hasEntry(attr, value));
        MatcherAssert.assertThat(
            new Attributes(attrs),
            Matchers.hasEntry(attr, value)
        );
    }

    /**
     * Attributes can build expected keys.
     * @throws Exception If some problem inside
     */
    @Test
    public void buildsExpectedKeys() throws Exception {
        final String attr = "attr-13";
        final String value = "some value \u20ac";
        MatcherAssert.assertThat(
            new Attributes().with(attr, value).asKeys(),
            Matchers.hasEntry(
                attr,
                new ExpectedAttributeValue(new AttributeValue(value))
            )
        );
    }

    /**
     * Attributes can filter out unnecessary keys.
     * @throws Exception If some problem inside
     */
    @Test
    public void filtersOutUnnecessaryKeys() throws Exception {
        MatcherAssert.assertThat(
            new Attributes()
                .with("first", "test-1")
                .with("second", "test-2")
                .only(Collections.singletonList("never"))
                .keySet(),
            Matchers.empty()
        );
    }

    /**
     * Attributes should be case-sensitive.
     * @throws Exception If some problem inside
     */
    @Test
    public void caseSensitive() throws Exception {
        final String first = "Alpha";
        final String second = "AlPha";
        final String third = "Beta";
        MatcherAssert.assertThat(
            new Attributes().with(
                new ArrayMap<String, AttributeValue>()
                    .with("Gamma", new AttributeValue(""))
                    .with("gAMma", new AttributeValue(""))
            ).keySet(),
            Matchers.hasSize(2)
        );
        MatcherAssert.assertThat(
            new Attributes()
                .with(first, "val-1")
                .with(second, "val-2"),
            Matchers.allOf(
                Matchers.hasKey(first),
                Matchers.hasKey(second)
            )
        );
        MatcherAssert.assertThat(
            new Attributes()
                .with(third, "some text to use")
                .only(Collections.singletonList(third)),
            Matchers.hasKey(third)
        );
    }

}
