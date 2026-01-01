/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.jcabi.aspects.Immutable;
import java.io.IOException;
import java.util.Map;

/**
 * Immutable Amazon DynamoDB item.
 *
 * <p>The class is immutable, which means that every call to
 * {@link #put(String,AttributeValueUpdate)} or {@link #put(Map)} changes
 * data in Amazon, but doesn't change the object. The object will contain
 * dirty data right after PUT operation, and should not be used any more.
 *
 * @since 0.1
 */
@Immutable
public interface Item {

    /**
     * Get one attribute, fetching directly from AWS (runtime exception if
     * the attribute is absent, use {@link #has(String)} first).
     * @param name Attribute name
     * @return Value
     * @throws IOException In case of DynamoDB failure
     */
    AttributeValue get(String name) throws IOException;

    /**
     * Does this attribute exist?
     * @param name Attribute name
     * @return TRUE if it exists
     * @throws IOException In case of DynamoDB failure
     */
    boolean has(String name)
        throws IOException;

    /**
     * Change one attribute, immediately saving it to AWS (all other attributes
     * will be set to NULL, except primary keys).
     *
     * <p>Data in memory will become out of sync right after a successful
     * execution of the method.
     *
     * @param name Attribute name
     * @param value Value to save
     * @return Values saved
     * @throws IOException In case of DynamoDB failure
     * @since 0.12
     */
    Map<String, AttributeValue> put(String name, AttributeValueUpdate value)
        throws IOException;

    /**
     * Change all attributes in one call.
     *
     * <p>Data in memory will become out of sync right after a successful
     * execution of the method.
     *
     * <p>It is recommended to use {@link AttributeUpdates} supplementary class,
     * instead of a raw {@link Map}.
     *
     * @param attrs Attributes
     * @return Values saved
     * @throws IOException In case of DynamoDB failure
     * @since 0.12
     */
    Map<String, AttributeValue> put(Map<String, AttributeValueUpdate> attrs)
        throws IOException;

    /**
     * Get back to the frame it is from.
     * @return Frame
     */
    Frame frame();

}
