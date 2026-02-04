/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Immutable;
import java.io.IOException;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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
 *   System.out.println("Name: " + item.get("name").s());
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
