/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TermsAggregationModeTests extends ESTestCase {

    public void testWriteAndRead() throws IOException {
        for (TermsAggregationMode mode : TermsAggregationMode.values()) {
            BytesStreamOutput out = new BytesStreamOutput();
            mode.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            assertThat(TermsAggregationMode.readFromStream(in), equalTo(mode));
        }
    }

    public void testParseApproximate() {
        assertThat(TermsAggregationMode.parse("approximate"), equalTo(TermsAggregationMode.APPROXIMATE));
    }

    public void testParseExact() {
        assertThat(TermsAggregationMode.parse("exact"), equalTo(TermsAggregationMode.EXACT));
    }

    public void testParseInvalid() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> TermsAggregationMode.parse("invalid"));
        assertThat(e.getMessage(), equalTo("Unknown terms aggregation mode: [invalid]. Valid values are [approximate, exact]"));
    }

    public void testDefaultIsApproximate() {
        assertThat(TermsAggregationMode.DEFAULT, equalTo(TermsAggregationMode.APPROXIMATE));
    }

    public void testToString() {
        assertThat(TermsAggregationMode.APPROXIMATE.toString(), equalTo("approximate"));
        assertThat(TermsAggregationMode.EXACT.toString(), equalTo("exact"));
    }
}
