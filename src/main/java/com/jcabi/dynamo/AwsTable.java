/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

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
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

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
        final DynamoDbClient aws = this.credentials.aws();
        try {
            final PutItemRequest request = PutItemRequest.builder()
                .tableName(this.self)
                .item(attributes)
                .returnValues(ReturnValue.NONE)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build();
            final PutItemResponse result = aws.putItem(request);
            final long start = System.currentTimeMillis();
            Logger.info(
                this, "#put('%[text]s'): created item in '%s', %s, in %[ms]s",
                attributes, this.self,
                new PrintableConsumedCapacity(
                    result.consumedCapacity()
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
        } catch (final SdkClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to put into \"%s\" with %s",
                    this.self, attributes
                ),
                ex
            );
        } finally {
            aws.close();
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
        final DynamoDbClient aws = this.credentials.aws();
        try {
            final long start = System.currentTimeMillis();
            final DescribeTableResponse result = aws.describeTable(
                DescribeTableRequest.builder().tableName(this.self).build()
            );
            final Collection<String> keys = new LinkedList<>();
            for (final KeySchemaElement key
                : result.table().keySchema()) {
                keys.add(key.attributeName());
            }
            Logger.info(
                this, "#keys(): table %s described, in %[ms]s",
                this.self, System.currentTimeMillis() - start
            );
            return keys;
        } catch (final SdkClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to describe \"%s\"",
                    this.self
                ),
                ex
            );
        } finally {
            aws.close();
        }
    }

    @Override
    public void delete(final Map<String, AttributeValue> attributes)
        throws IOException {
        final DynamoDbClient aws = this.credentials.aws();
        try {
            final DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(this.self)
                .key(attributes)
                .returnValues(ReturnValue.NONE)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build();
            final DeleteItemResponse result = aws.deleteItem(request);
            final long start = System.currentTimeMillis();
            Logger.info(
                this,
                "#delete('%[text]s'): deleted item in '%s', %s, in %[ms]s",
                attributes, this.self,
                new PrintableConsumedCapacity(
                    result.consumedCapacity()
                ).print(),
                System.currentTimeMillis() - start
            );
        } catch (final SdkClientException ex) {
            throw new IOException(
                String.format(
                    "Failed to delete at \"%s\" by keys %s",
                    this.self, attributes
                ),
                ex
            );
        } finally {
            aws.close();
        }
    }
}
