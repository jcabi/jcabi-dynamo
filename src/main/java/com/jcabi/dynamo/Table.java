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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.jcabi.aspects.Immutable;
import java.io.IOException;
import java.util.Map;

/**
 * Amazon DynamoDB table abstraction.
 *
 * <p>To get data from the table use {@link Table#frame()} method. To
 * create a new item in the table (or replace the existing one) use
 * {@link Table#put(Attributes)} method. For example:
 *
 * <pre> Region region = new Region.Simple(...);
 * Table table = region.table("employees");
 * table.put(new Attributes().with("name", "John Smith"));
 * for (Item item : table.frame()) {
 *   System.out.println("Name: " + item.get("name").getS());
 * }
 * table.frame()
 *   .where("name", Conditions.equalTo("John Smith"))
 *   .iterator().next().remove();</pre>
 *
 * @since 0.1
 */
@Immutable
public interface Table {

    /**
     * Put new item there.
     *
     * <p>It is recommended to use {@link Attributes} supplementary class,
     * instead of a raw {@link Map}.
     *
     * @param attributes Attributes to save
     * @return Item just created
     * @throws IOException In case of DynamoDB failure
     * @see Attributes
     */
    Item put(Map<String, AttributeValue> attributes) throws IOException;

    /**
     * Make a new frame, in order to retrieve items.
     * @return Frame
     */
    Frame frame();

    /**
     * Get back to the entire region.
     * @return Region
     */
    Region region();

    /**
     * Get real table name.
     * @return Actual name of DynamoDB table
     */
    String name();

    /**
     * Delete item from aws table.
     * <p>It is recommended to use {@link Attributes} supplementary class,
     * instead of a raw {@link Map}.
     * @param attributes Attributes containing item key and value
     * @throws IOException In case of DynamoDB failure
     * @see Attributes
     */
    void delete(Map<String, AttributeValue> attributes) throws IOException;

}
