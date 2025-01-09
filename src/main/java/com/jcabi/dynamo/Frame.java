/*
 * Copyright (c) 2012-2025, jcabi.com
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

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.jcabi.aspects.Immutable;
import java.util.Collection;
import java.util.Map;

/**
 * DynamoDB frame (subset of a table).
 *
 * <p>{@link Frame} is a subset of a Dynamo table, and is used to retrieve items
 * and remove them. {@link Frame} acts as an iterable immutable collection of
 * items. You can't use {@link Frame#remove(Object)} method directly. Instead,
 * find the right item using iterator and than remove it with
 * {@link java.util.Iterator#remove()}.
 *
 * <p>To fetch items from Dynamo DB, {@link Frame} uses
 * {@code Query} operation, with "consistent read" mode turned ON. It fetches
 * twenty items on every request.
 *
 * <p>Keep in mind that Frame object provides a very limited functionality
 * and is intended to be used in most cases, but not in all of them. When
 * you need something specific, just get an Amazon DynamoDB client from
 * a {@link Region} and use Amazon SDK methods directly.
 *
 * @since 0.1
 * @see Item
 * @see <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html">Query and Scan</a>
 */
@Immutable
public interface Frame extends Collection<Item> {

    /**
     * Refine using this EQ condition argument.
     * @param name Attribute name
     * @param value String value expected
     * @return New frame
     * @since 0.7.21
     */
    Frame where(String name, String value);

    /**
     * Refine using this condition.
     *
     * <p>It is recommended to use a utility static method
     * {@link Conditions#equalTo(Object)}, when condition is simply an
     * equation to a plain string value.
     *
     * @param name Attribute name
     * @param condition The condition
     * @return New frame
     */
    Frame where(String name, Condition condition);

    /**
     * Refine using these conditions.
     *
     * <p>It is recommended to use {@link Conditions} supplementary class
     * instead of a raw {@link Map}.
     *
     * @param conditions The conditions
     * @return New frame
     * @see Conditions
     */
    Frame where(Map<String, Condition> conditions);

    /**
     * Get back to the table this frame came from.
     * @return The table
     */
    Table table();

    /**
     * Change valve for items fetching.
     * @param valve The valve to go through
     * @return New frame
     * @since 0.7.21
     */
    Frame through(Valve valve);

}
