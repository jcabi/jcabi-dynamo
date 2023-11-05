/*
 * Copyright (c) 2012-2023, jcabi.com
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
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.jcabi.aspects.Cacheable;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.immutable.Array;
import com.jcabi.log.Logger;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Single table in Dynamo, through AWS SDK.
 *
 * @since 0.1
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = { "credentials", "reg", "self" })
@SuppressWarnings("PMD.GuardLogStatement")
final class AwsTable implements Table {

    /**
     * AWS credentials.
     */
    private final transient Credentials credentials;

    /**
     * Region.
     */
    private final transient Region reg;

    /**
     * Table name.
     */
    private final transient String self;

    /**
     * Public ctor.
     * @param creds Credentials
     * @param region Region
     * @param table Table name
     */
    AwsTable(final Credentials creds, final Region region,
        final String table) {
        this.credentials = creds;
        this.reg = region;
        this.self = table;
    }

    @Override
    public Item put(final Map<String, AttributeValue> attributes)
        throws IOException {
        final AmazonDynamoDB aws = this.credentials.aws();
        try {
            final PutItemRequest request = new PutItemRequest();
            request.setTableName(this.self);
            request.setItem(attributes);
            request.setReturnValues(ReturnValue.NONE);
            request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            final PutItemResult result = aws.putItem(request);
            final long start = System.currentTimeMillis();
            Logger.info(
                this, "#put('%[text]s'): created item in '%s', %s, in %[ms]s",
                attributes, this.self,
                new PrintableConsumedCapacity(
                    result.getConsumedCapacity()
                ).print(),
                System.currentTimeMillis() - start
            );
            return new AwsItem(
                this.credentials,
                this.frame(),
                this.self,
                new Attributes(attributes).only(this.keys()),
                new Array<>(this.keys())
            );
        } catch (final AmazonClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to put into \"%s\" with %s",
                    this.self, attributes
                ),
                ex
            );
        } finally {
            aws.shutdown();
        }
    }

    @Override
    public Region region() {
        return this.reg;
    }

    @Override
    public AwsFrame frame() {
        return new AwsFrame(this.credentials, this, this.self);
    }

    @Override
    public String name() {
        return this.self;
    }

    /**
     * Get names of keys.
     * @return Names of attributes, which are primary keys
     * @throws IOException If DynamoDB fails
     */
    @Cacheable(forever = true)
    public Collection<String> keys() throws IOException {
        final AmazonDynamoDB aws = this.credentials.aws();
        try {
            final long start = System.currentTimeMillis();
            final DescribeTableResult result = aws.describeTable(
                new DescribeTableRequest().withTableName(this.self)
            );
            final Collection<String> keys = new LinkedList<>();
            for (final KeySchemaElement key
                : result.getTable().getKeySchema()) {
                keys.add(key.getAttributeName());
            }
            Logger.info(
                this, "#keys(): table %s described, in %[ms]s",
                this.self, System.currentTimeMillis() - start
            );
            return keys;
        } catch (final AmazonClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to describe \"%s\"",
                    this.self
                ),
                ex
            );
        } finally {
            aws.shutdown();
        }
    }

    @Override
    public void delete(final Map<String, AttributeValue> attributes)
        throws IOException {
        final AmazonDynamoDB aws = this.credentials.aws();
        try {
            final DeleteItemRequest request = new DeleteItemRequest();
            request.setTableName(this.self);
            request.setKey(attributes);
            request.setReturnValues(ReturnValue.NONE);
            request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            final DeleteItemResult result = aws.deleteItem(request);
            final long start = System.currentTimeMillis();
            Logger.info(
                this,
                "#delete('%[text]s'): deleted item in '%s', %s, in %[ms]s",
                attributes, this.self,
                new PrintableConsumedCapacity(
                    result.getConsumedCapacity()
                ).print(),
                System.currentTimeMillis() - start
            );
        } catch (final AmazonClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to delete at \"%s\" by keys %s",
                    this.self, attributes
                ),
                ex
            );
        } finally {
            aws.shutdown();
        }
    }
}
