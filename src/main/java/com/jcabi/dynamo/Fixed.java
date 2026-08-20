/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.jcabi.aspects.Immutable;
import com.jcabi.immutable.Array;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Dosage with fixed list of items.
 * @since 0.1
 */
@Immutable
final class Fixed implements Dosage {

    /**
     * List of items.
     */
    private final transient Array<Map<String, AttributeValue>> list;

    /**
     * Previous dosage.
     */
    private final transient Dosage prev;

    /**
     * Ctor.
     * @param dsg Dosage
     * @param items Items
     */
    Fixed(final Dosage dsg, final List<Map<String, AttributeValue>> items) {
        this.prev = dsg;
        this.list = new Array<>(items);
    }

    @Override
    public List<Map<String, AttributeValue>> items() {
        return Collections.unmodifiableList(this.list);
    }

    @Override
    public Dosage next() {
        return this.prev.next();
    }

    @Override
    public boolean hasNext() {
        return this.prev.hasNext();
    }
}
