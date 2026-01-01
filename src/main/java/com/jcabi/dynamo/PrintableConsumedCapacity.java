/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo;

import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;

/**
 * Printable consumed capacity.
 *
 * @since 0.22
 */
final class PrintableConsumedCapacity {

    /**
     * Consumed capacity.
     */
    private final ConsumedCapacity capacity;

    /**
     * Default ctor.
     * @param capacity Consumed capacity
     */
    PrintableConsumedCapacity(final ConsumedCapacity capacity) {
        this.capacity = capacity;
    }

    /**
     * Print consumed capacity nicely.
     * @return Suffix to add to a log line
     */
    public String print() {
        final String txt;
        if (this.capacity == null) {
            txt = "";
        } else {
            txt = String.format("%.2f units", this.capacity.getCapacityUnits());
        }
        return txt;
    }
}
