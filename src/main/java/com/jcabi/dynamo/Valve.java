/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2025 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.jcabi.aspects.Immutable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Provider of dosages of DynamoDB items.
 *
 * @since 0.7.21
 */
@Immutable
public interface Valve {

    /**
     * Fetch the first dosage.
     * @param credentials Credentials to AWS
     * @param table Table name
     * @param conditions Conditions
     * @param keys Keys of the table
     * @return Dosage
     * @throws IOException In case of DynamoDB failure
     * @checkstyle ParameterNumber (5 lines)
     */
    Dosage fetch(Credentials credentials, String table,
        Map<String, Condition> conditions, Collection<String> keys)
        throws IOException;

    /**
     * Count items.
     * @param credentials Credentials to AWS
     * @param table Table name
     * @param conditions Conditions
     * @return Total count of the
     * @throws IOException In case of DynamoDB failure
     */
    int count(Credentials credentials, String table,
        Map<String, Condition> conditions) throws IOException;

}
