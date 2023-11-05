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
