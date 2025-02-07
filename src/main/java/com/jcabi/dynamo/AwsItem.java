/*
 * Copyright (c) 2012-2025 Yegor Bugayenko
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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
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

/**
 * Single item/row in a DynamoDB table.
 *
 * @since 0.1
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = { "credentials", "frm", "name", "attributes" })
@SuppressWarnings("PMD.GuardLogStatement")
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
            final AmazonDynamoDB aws = this.credentials.aws();
            try {
                final GetItemRequest request = this.makeItemRequestFor(attr);
                final long start = System.currentTimeMillis();
                final GetItemResult result = aws.getItem(request);
                has = result.getItem().get(attrib) != null;
                Logger.info(
                    this, "#has('%s'): %B from DynamoDB, %s, in %[ms]s",
                    attr, has,
                    new PrintableConsumedCapacity(
                        result.getConsumedCapacity()
                    ).print(),
                    System.currentTimeMillis() - start
                );
            } catch (final AmazonClientException ex) {
                throw new IOException(
                    String.format(
                        "Failed to check existence of \"%s\" at \"%s\" by %s",
                        attr, this.name, this.keys
                    ),
                    ex
                );
            } finally {
                aws.shutdown();
            }
        }
        return has;
    }

    @Override
    public AttributeValue get(final String attr) throws IOException {
        final String attrib = attr;
        AttributeValue value = this.attributes.get(attrib);
        if (value == null) {
            final AmazonDynamoDB aws = this.credentials.aws();
            try {
                final GetItemRequest request = this.makeItemRequestFor(attrib);
                final long start = System.currentTimeMillis();
                final GetItemResult result = aws.getItem(request);
                value = result.getItem().get(attrib);
                Logger.info(
                    this,
                    // @checkstyle LineLength (1 line)
                    "#get('%s'): loaded '%[text]s' from DynamoDB, %s, in %[ms]s",
                    attrib, value,
                    new PrintableConsumedCapacity(
                        result.getConsumedCapacity()
                    ).print(),
                    System.currentTimeMillis() - start
                );
            } catch (final AmazonClientException ex) {
                throw new IOException(
                    String.format(
                        "Failed to get \"%s\" from \"%s\" by %s",
                        attr, this.name, this.keys
                    ),
                    ex
                );
            } finally {
                aws.shutdown();
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
        final AmazonDynamoDB aws = this.credentials.aws();
        final Attributes expected = this.attributes.only(this.keys);
        try {
            final UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(this.name)
                .withExpected(expected.asKeys())
                .withKey(expected)
                .withAttributeUpdates(attrs)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withReturnValues(ReturnValue.UPDATED_NEW);
            final long start = System.currentTimeMillis();
            final UpdateItemResult result = aws.updateItem(request);
            Logger.info(
                this, "#put('%s'): updated item to DynamoDB, %s, in %[ms]s",
                attrs,
                new PrintableConsumedCapacity(
                    result.getConsumedCapacity()
                ).print(),
                System.currentTimeMillis() - start
            );
            return result.getAttributes();
        } catch (final AmazonClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to put %s into \"%s\" with %s",
                    attrs, this.name, this.keys
                ),
                ex
            );
        } finally {
            aws.shutdown();
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
        final GetItemRequest request = new GetItemRequest();
        request.setTableName(this.name);
        request.setAttributesToGet(Collections.singletonList(attr));
        request.setKey(this.attributes.only(this.keys));
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        request.setConsistentRead(true);
        return request;
    }

}
