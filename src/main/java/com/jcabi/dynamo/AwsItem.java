/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.Array;
import com.jcabi.log.Logger;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Single item/row in a DynamoDB table.
 *
 * @since 0.1
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = { "credentials", "frm", "name", "attributes" })
final class AwsItem implements Item {

    /**
     * AWS credentials.
     */
    private final transient Credentials credentials;

    /**
     * Frame.
     */
    private final transient AwsFrame frm;

    /**
     * Table name.
     */
    private final transient String name;

    /**
     * Pre-loaded attributes and their values.
     */
    private final transient Attributes attributes;

    /**
     * Table keys.
     */
    private final transient Array<String> keys;

    /**
     * Public ctor.
     * @param creds Credentials
     * @param frame Frame
     * @param table Table name
     * @param attrs Loaded already attributes with values
     * @param pks Keys of the table
     * @checkstyle ParameterNumber (5 lines)
     */
    AwsItem(final Credentials creds, final AwsFrame frame,
        final String table, final Attributes attrs,
        final Array<String> pks) {
        this.credentials = creds;
        this.frm = frame;
        this.name = table;
        this.attributes = attrs;
        this.keys = pks;
    }

    @Override
    public boolean has(final String attr) throws IOException {
        final String attrib = attr;
        boolean has = this.attributes.containsKey(attrib);
        if (!has) {
            final DynamoDbClient aws = this.credentials.aws();
            try {
                final GetItemResponse result = aws.getItem(
                    this.makeItemRequestFor(attr)
                );
                has = result.item().get(attrib) != null;
                Logger.info(
                    this, "#has('%s'): %B from DynamoDB, %s",
                    attr, has,
                    new PrintableConsumedCapacity(
                        result.consumedCapacity()
                    ).print()
                );
            } catch (final SdkClientException ex) {
                throw new IOException(
                    String.format(
                        "Failed to check existence of \"%s\" at \"%s\" by %s",
                        attr, this.name, this.keys
                    ),
                    ex
                );
            } finally {
                aws.close();
            }
        }
        return has;
    }

    @Override
    public AttributeValue get(final String attr) throws IOException {
        final String attrib = attr;
        AttributeValue value = this.attributes.get(attrib);
        if (value == null) {
            final DynamoDbClient aws = this.credentials.aws();
            try {
                final GetItemResponse result = aws.getItem(
                    this.makeItemRequestFor(attrib)
                );
                value = result.item().get(attrib);
                Logger.info(
                    this,
                    "#get('%s'): loaded '%[text]s' from DynamoDB, %s",
                    attrib, value,
                    new PrintableConsumedCapacity(
                        result.consumedCapacity()
                    ).print()
                );
            } catch (final SdkClientException ex) {
                throw new IOException(
                    String.format(
                        "Failed to get \"%s\" from \"%s\" by %s",
                        attr, this.name, this.keys
                    ),
                    ex
                );
            } finally {
                aws.close();
            }
        }
        if (value == null) {
            throw new NoSuchElementException(
                String.format("attribute \"%s\" not found", attr)
            );
        }
        return value;
    }

    @Override
    public Map<String, AttributeValue> put(final String attr,
        final AttributeValueUpdate value) throws IOException {
        return this.put(new AttributeUpdates().with(attr, value));
    }

    @Override
    public Map<String, AttributeValue> put(
        final Map<String, AttributeValueUpdate> attrs) throws IOException {
        final DynamoDbClient aws = this.credentials.aws();
        final Attributes expected = this.attributes.only(this.keys);
        try {
            final UpdateItemResponse result = aws.updateItem(
                UpdateItemRequest.builder()
                    .tableName(this.name)
                    .expected(expected.asKeys())
                    .key(expected)
                    .attributeUpdates(attrs)
                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .returnValues(ReturnValue.UPDATED_NEW)
                    .build()
            );
            Logger.info(
                this, "#put('%s'): updated item to DynamoDB, %s",
                attrs,
                new PrintableConsumedCapacity(
                    result.consumedCapacity()
                ).print()
            );
            return result.attributes();
        } catch (final SdkClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to put %s into \"%s\" with %s",
                    attrs, this.name, this.keys
                ),
                ex
            );
        } finally {
            aws.close();
        }
    }

    @Override
    public Frame frame() {
        return this.frm;
    }

    /**
     * Makes a GetItemRequest for a given attribute.
     * @param attr Attribute name
     * @return GetItemRequest
     */
    private GetItemRequest makeItemRequestFor(final String attr) {
        return GetItemRequest.builder()
            .tableName(this.name)
            .attributesToGet(Collections.singletonList(attr))
            .key(this.attributes.only(this.keys))
            .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .consistentRead(true)
            .build();
    }

}
