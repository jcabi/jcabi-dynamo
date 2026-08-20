/*
 * SPDX-FileCopyrightText: Copyright (c) 2012-2026 Yegor Bugayenko
 * SPDX-License-Identifier: MIT
 */
package com.jcabi.dynamo.mock;

import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;

/**
 * One condition of a SELECT.
 * @since 0.10
 */
@ToString
@EqualsAndHashCode(of = "cond")
final class Clause {

    /**
     * Condition to render.
     */
    private final transient Map.Entry<String, Condition> cond;

    /**
     * Public ctor.
     * @param cnd Condition to render
     */
    Clause(final Map.Entry<String, Condition> cnd) {
        this.cond = cnd;
    }

    /**
     * Render it as SQL.
     * @return SQL fragment
     */
    String sql() {
        final String operator =
            this.cond.getValue().comparisonOperatorAsString();
        final String opr;
        if (operator.equals(ComparisonOperator.GT.toString())) {
            opr = ">";
        } else if (operator.equals(ComparisonOperator.LT.toString())) {
            opr = "<";
        } else if (operator.equals(ComparisonOperator.EQ.toString())) {
            opr = "=";
        } else {
            throw new UnsupportedOperationException(
                String.format(
                    "Only EQ/GT/LT operators are supported at the moment: %s",
                    operator
                )
            );
        }
        return String.format("`%s` %s ?", this.cond.getKey(), opr);
    }
}
