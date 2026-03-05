/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;

/**
 * Coordinator-side logic for TPUT (Three-Phase Uniform Threshold) algorithm.
 * <p>
 * After Phase 1 reduce, this coordinator examines the reduced terms aggregation
 * to determine if refinement is needed. If {@code needsRefinement()} is true,
 * it computes the threshold T and orchestrates Phase 2 shard requests.
 * <p>
 * The TPUT threshold T = τ₁ / m, where:
 * <ul>
 *   <li>τ₁ = minimum doc_count among the provisional top-k from Phase 1</li>
 *   <li>m = number of shards that participated in Phase 1</li>
 * </ul>
 * Any term with a true global doc_count that could potentially be in the top-k
 * must have at least T documents on some shard. By collecting all terms with
 * doc_count >= T from every shard, we guarantee no qualifying term is missed.
 */
public final class TermsRefinementCoordinator {

    private TermsRefinementCoordinator() {}

    /**
     * Checks if any top-level terms aggregation in the reduced results needs TPUT refinement.
     * <p>
     * Note: only top-level aggregations are scanned. Nested terms aggregations (sub-aggregations)
     * with {@code mode=EXACT} are not currently detected and will not trigger refinement.
     */
    public static List<RefinementTarget> findRefinementTargets(InternalAggregations aggregations) {
        List<RefinementTarget> targets = new ArrayList<>();
        for (InternalAggregation agg : aggregations.asList()) {
            if (agg instanceof AbstractInternalTerms<?, ?> terms && terms.needsRefinement()) {
                long threshold = computeThreshold(terms.getProvisionalMinDocCount(), terms.getNumShardsInReduce());
                targets.add(new RefinementTarget(terms.getName(), threshold, terms.getRequiredSize()));
            }
        }
        return targets;
    }

    /**
     * Computes the TPUT threshold T = τ₁ / m.
     * Uses floor division. If τ₁ or m is invalid, returns 1 as a safe minimum.
     */
    static long computeThreshold(long provisionalMinDocCount, int numShards) {
        if (provisionalMinDocCount <= 0 || numShards <= 0) {
            return 1;
        }
        // Floor division: T = τ₁ / m
        // A term with global count >= τ₁ must appear on at least one shard
        // with count >= τ₁ / m (by pigeonhole principle)
        return Math.max(1, provisionalMinDocCount / numShards);
    }

    /**
     * Describes a single terms aggregation that needs refinement.
     */
    public record RefinementTarget(String aggregationName, long threshold, int requiredSize) {}
}
