/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Service that handles TPUT Phase 2/3 refinement logic.
 * <p>
 * Given a threshold T, modifies the aggregation configuration to collect
 * all terms with doc_count >= T (instead of the normal top-k selection)
 * and re-executes the aggregation.
 */
public final class TermsRefinementService {

    private TermsRefinementService() {}

    /**
     * Creates a modified SearchSourceBuilder for Phase 2 execution.
     * The terms aggregation is modified to use min_doc_count = threshold
     * and size/shard_size = Integer.MAX_VALUE to collect all qualifying terms.
     *
     * @param original The original search source from Phase 1
     * @param aggregationName The name of the terms aggregation to refine
     * @param threshold The TPUT threshold T = τ₁ / m
     * @return A new SearchSourceBuilder configured for Phase 2
     */
    public static SearchSourceBuilder createRefinementSource(SearchSourceBuilder original, String aggregationName, long threshold) {
        SearchSourceBuilder refinementSource = original.shallowCopy();
        // Disable hits — we only need aggregation results
        refinementSource.size(0);

        AggregatorFactories.Builder aggBuilder = original.aggregations();
        if (aggBuilder == null) {
            throw new IllegalStateException("No aggregations in search source for refinement of [" + aggregationName + "]");
        }

        // Find and deep-copy the target terms aggregation with modified parameters
        boolean found = false;
        // Build a new SearchSourceBuilder with only the target aggregation
        SearchSourceBuilder result = original.shallowCopy();
        result.size(0);
        // Reset aggregations by creating a new source without them, then re-adding
        SearchSourceBuilder finalSource = new SearchSourceBuilder();
        finalSource.query(result.query());
        finalSource.size(0);
        for (AggregationBuilder agg : aggBuilder.getAggregatorFactories()) {
            if (agg.getName().equals(aggregationName) && agg instanceof TermsAggregationBuilder) {
                // Deep copy the terms aggregation, modifying the target
                AggregationBuilder refined = AggregationBuilder.deepCopy(agg, copy -> {
                    if (copy instanceof TermsAggregationBuilder terms && copy.getName().equals(aggregationName)) {
                        // Set min_doc_count to threshold so only terms above T are returned
                        terms.minDocCount(threshold);
                        // Set size to max so we get ALL qualifying terms, not just top-k
                        terms.size(Integer.MAX_VALUE);
                        terms.shardSize(Integer.MAX_VALUE);
                        // Keep mode as APPROXIMATE for the refinement query — we don't want
                        // recursive refinement
                        terms.mode(TermsAggregationMode.APPROXIMATE);
                    }
                    return copy;
                });
                finalSource.aggregation(refined);
                found = true;
            }
            // Skip non-target aggregations to reduce work
        }
        if (found == false) {
            throw new IllegalStateException("Terms aggregation [" + aggregationName + "] not found in search source");
        }
        return finalSource;
    }

    /**
     * Merges Phase 2 refinement results from all shards.
     * The Phase 2 results contain exact counts because every shard reported
     * every term above threshold T (by TPUT guarantee).
     *
     * @param phase2Results List of per-shard refinement results from Phase 2
     * @param aggregationName The name of the terms aggregation
     * @return The merged aggregation results with exact counts
     */
    public static InternalAggregations mergeRefinementResults(
        List<TermsRefinementShardResponse> phase2Results,
        String aggregationName
    ) {
        // Collect all Phase 2 terms aggregations for reduction
        List<InternalAggregation> termsToReduce = new ArrayList<>();
        for (TermsRefinementShardResponse response : phase2Results) {
            InternalAggregation termsAgg = response.aggregations().get(aggregationName);
            if (termsAgg != null) {
                termsToReduce.add(termsAgg);
            }
        }

        if (termsToReduce.isEmpty()) {
            return InternalAggregations.EMPTY;
        }

        // The Phase 2 results come from all shards with threshold-filtered terms.
        // When reduced together, they produce exact counts (docCountError = 0)
        // because every shard reported every term above threshold T.
        return InternalAggregations.from(termsToReduce);
    }
}
