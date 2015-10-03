/**
 * Copyright (c) 2012-2015, jcabi.com
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
package com.jcabi.dynamo.retry;

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import com.jcabi.aspects.RetryOnFailure;
import com.jcabi.aspects.Tv;
import com.jcabi.dynamo.Credentials;
import com.jcabi.dynamo.Dosage;
import com.jcabi.dynamo.Valve;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Valve that retries on failure.
 *
 * @author Yegor Bugayenko (yegor@tpc2.com)
 * @version $Id$
 * @since 0.9
 */
@Immutable
@Loggable(Loggable.DEBUG)
@ToString
@EqualsAndHashCode(of = "origin")
public final class ReValve implements Valve {

    /**
     * Original valve.
     */
    private final transient Valve origin;

    /**
     * Public ctor.
     * @param valve Origin valve
     */
    public ReValve(
        @NotNull(message = "attribute valve cannot be null")
        final Valve valve) {
        this.origin = valve;
    }

    /**
     * {@inheritDoc}
     * @checkstyle ParameterNumber (7 lines)
     */
    @Override
    @RetryOnFailure(verbose = false, delay = Tv.FIVE, unit = TimeUnit.SECONDS)
    @NotNull(message = "Dosage cannot be null")
    public Dosage fetch(
        @NotNull(message = "attribute credentials cannot be null")
        final Credentials credentials,
        @NotNull(message = "attribute table cannot be null")
        final String table,
        @NotNull(message = "attribute conditions cannot be null")
        final Map<String, Condition> conditions,
        @NotNull(message = "attribute keys cannot be null")
        final Collection<String> keys) throws IOException {
        return new ReDosage(
            this.origin.fetch(credentials, table, conditions, keys)
        );
    }

}
