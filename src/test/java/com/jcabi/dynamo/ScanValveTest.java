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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test case for {@link ScanValve}.
 * @since 0.1
 */
final class ScanValveTest {

    @Test
    @SuppressWarnings("unchecked")
    void fetchesData() throws Exception {
        final Valve valve = new ScanValve();
        final Credentials credentials = Mockito.mock(Credentials.class);
        final ImmutableMap<String, AttributeValue> item =
            new ImmutableMap.Builder<String, AttributeValue>()
                .build();
        final AmazonDynamoDB aws = Mockito.mock(AmazonDynamoDB.class);
        Mockito.doReturn(aws).when(credentials).aws();
        Mockito.doReturn(
            new ScanResult()
                .withItems(
                    Collections.singletonList(item)
            )
                .withConsumedCapacity(
                    new ConsumedCapacity().withCapacityUnits(1d)
                )
        ).when(aws).scan(Mockito.any(ScanRequest.class));
        final Dosage dosage = valve.fetch(
            credentials, "table",
            new Conditions(), new ArrayList<>(0)
        );
        MatcherAssert.assertThat(dosage.hasNext(), Matchers.is(false));
        MatcherAssert.assertThat(dosage.items(), Matchers.hasItem(item));
    }

}
