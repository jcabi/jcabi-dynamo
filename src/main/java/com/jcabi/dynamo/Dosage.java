/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Immutable;
import com.jcabi.aspects.Loggable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Dosage of items retrieved from table.
 *
 * @since 0.1
 */
@Immutable
public interface Dosage {

    /**
     * Items.
     * @return List of items
     */
    List<Map<String, AttributeValue>> items();

    /**
     * Has next dosage?
     * @return TRUE if next storage is available
     */
    boolean hasNext();

    /**
     * Fetch next dosage.
     * @return The dosage
     */
    Dosage next();

    /**
     * Always empty.
     *
     * @since 0.1
     */
    @Immutable
    @Loggable(Loggable.DEBUG)
    @ToString
    @EqualsAndHashCode
    final class Empty implements Dosage {
        @Override
        public List<Map<String, AttributeValue>> items() {
            return new ArrayList<>(0);
        }

        @Override
        public Dosage next() {
            throw new IllegalStateException(
                "this is nothing left"
            );
        }

        @Override
        public boolean hasNext() {
            return false;
        }
    }

}
