/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Locale;

/**
 * Determines the accuracy mode for terms aggregation.
 * <p>
 * {@link #APPROXIMATE} is the default single-round behavior with potential error.
 * {@link #EXACT} uses the TPUT (Three-Phase Uniform Threshold) algorithm for
 * guaranteed zero-error top-k results at the cost of additional round-trips.
 */
public enum TermsAggregationMode implements Writeable {
    APPROXIMATE(new ParseField("approximate")),
    EXACT(new ParseField("exact"));

    public static final TermsAggregationMode DEFAULT = APPROXIMATE;

    private final ParseField parseField;

    TermsAggregationMode(ParseField parseField) {
        this.parseField = parseField;
    }

    public ParseField parseField() {
        return parseField;
    }

    public static TermsAggregationMode parse(String value) {
        for (TermsAggregationMode mode : values()) {
            if (mode.parseField.match(value, org.elasticsearch.common.xcontent.LoggingDeprecationHandler.INSTANCE)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
            "Unknown terms aggregation mode: [" + value + "]. Valid values are [approximate, exact]"
        );
    }

    public static TermsAggregationMode readFromStream(StreamInput in) throws IOException {
        return in.readEnum(TermsAggregationMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
