/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TermsRefinementCoordinatorTests extends ESTestCase {

    public void testComputeThresholdBasic() {
        // T = τ₁ / m = 100 / 5 = 20
        assertThat(TermsRefinementCoordinator.computeThreshold(100, 5), equalTo(20L));
    }

    public void testComputeThresholdFloorDivision() {
        // T = τ₁ / m = 7 / 3 = 2 (floor division)
        assertThat(TermsRefinementCoordinator.computeThreshold(7, 3), equalTo(2L));
    }

    public void testComputeThresholdMinimumIsOne() {
        // When τ₁ < m, floor division gives 0, but minimum is 1
        assertThat(TermsRefinementCoordinator.computeThreshold(1, 10), equalTo(1L));
    }

    public void testComputeThresholdSingleShard() {
        // T = τ₁ / 1 = τ₁
        assertThat(TermsRefinementCoordinator.computeThreshold(50, 1), equalTo(50L));
    }

    public void testComputeThresholdInvalidInputs() {
        // Invalid inputs should return 1 as safe minimum
        assertThat(TermsRefinementCoordinator.computeThreshold(0, 5), equalTo(1L));
        assertThat(TermsRefinementCoordinator.computeThreshold(-1, 5), equalTo(1L));
        assertThat(TermsRefinementCoordinator.computeThreshold(100, 0), equalTo(1L));
        assertThat(TermsRefinementCoordinator.computeThreshold(100, -1), equalTo(1L));
    }

    public void testFindRefinementTargetsEmpty() {
        // No aggregations → no targets
        List<TermsRefinementCoordinator.RefinementTarget> targets = TermsRefinementCoordinator.findRefinementTargets(
            InternalAggregations.EMPTY
        );
        assertThat(targets, empty());
    }

    public void testFindRefinementTargetsNoRefinementNeeded() {
        // Terms aggregation with needsRefinement = false → no targets
        StringTerms terms = createStringTerms("my_terms", false);
        InternalAggregations aggs = InternalAggregations.from(List.of(terms));

        List<TermsRefinementCoordinator.RefinementTarget> targets = TermsRefinementCoordinator.findRefinementTargets(aggs);
        assertThat(targets, empty());
    }

    public void testFindRefinementTargetsWithRefinement() {
        // Terms aggregation with needsRefinement = true → produces target
        StringTerms terms = createStringTerms("my_terms", true);
        terms.setNeedsRefinement(true);
        terms.setProvisionalMinDocCount(100);
        terms.setNumShardsInReduce(5);
        InternalAggregations aggs = InternalAggregations.from(List.of(terms));

        List<TermsRefinementCoordinator.RefinementTarget> targets = TermsRefinementCoordinator.findRefinementTargets(aggs);
        assertThat(targets, hasSize(1));
        assertThat(targets.get(0).aggregationName(), equalTo("my_terms"));
        assertThat(targets.get(0).threshold(), equalTo(20L)); // 100 / 5
    }

    public void testRefinementTargetRecord() {
        TermsRefinementCoordinator.RefinementTarget target = new TermsRefinementCoordinator.RefinementTarget("test_agg", 42, 10);
        assertThat(target.aggregationName(), equalTo("test_agg"));
        assertThat(target.threshold(), equalTo(42L));
        assertThat(target.requiredSize(), equalTo(10));
    }

    private StringTerms createStringTerms(String name, boolean needsRefinement) {
        StringTerms terms = new StringTerms(
            name,
            BucketOrder.count(false),
            BucketOrder.count(false),
            10,
            1,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            true,
            0,
            Collections.emptyList(),
            0L
        );
        if (needsRefinement) {
            terms.setMode(TermsAggregationMode.EXACT);
        }
        return terms;
    }
}
