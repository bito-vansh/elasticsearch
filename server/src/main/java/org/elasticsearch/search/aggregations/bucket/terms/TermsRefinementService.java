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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
     * Creates a modified SearchSourceBuilder for Phase 3 gap resolution.
     * The terms aggregation is modified to use an include filter for only the
     * specific terms that need gap resolution on this shard.
     *
     * @param original The original search source from Phase 1
     * @param aggregationName The name of the terms aggregation to refine
     * @param termsToResolve The specific term keys to query for gap resolution
     * @return A new SearchSourceBuilder configured for Phase 3
     */
    public static SearchSourceBuilder createGapResolutionSource(
        SearchSourceBuilder original,
        String aggregationName,
        List<String> termsToResolve
    ) {
        AggregatorFactories.Builder aggBuilder = original.aggregations();
        if (aggBuilder == null) {
            throw new IllegalStateException("No aggregations in search source for gap resolution of [" + aggregationName + "]");
        }

        // Build the include set from the term keys
        TreeSet<BytesRef> includeValues = new TreeSet<>();
        for (String termKey : termsToResolve) {
            includeValues.add(new BytesRef(termKey));
        }
        IncludeExclude includeFilter = new IncludeExclude(null, null, includeValues, null);

        SearchSourceBuilder finalSource = new SearchSourceBuilder();
        finalSource.query(original.query());
        finalSource.size(0);

        boolean found = false;
        for (AggregationBuilder agg : aggBuilder.getAggregatorFactories()) {
            if (agg.getName().equals(aggregationName) && agg instanceof TermsAggregationBuilder) {
                AggregationBuilder refined = AggregationBuilder.deepCopy(agg, copy -> {
                    if (copy instanceof TermsAggregationBuilder terms && copy.getName().equals(aggregationName)) {
                        // Only collect the specific gap terms
                        terms.includeExclude(includeFilter);
                        // min_doc_count=1: include any term with at least 1 doc
                        terms.minDocCount(1);
                        terms.size(Integer.MAX_VALUE);
                        terms.shardSize(Integer.MAX_VALUE);
                        terms.mode(TermsAggregationMode.APPROXIMATE);
                    }
                    return copy;
                });
                finalSource.aggregation(refined);
                found = true;
            }
        }
        if (found == false) {
            throw new IllegalStateException("Terms aggregation [" + aggregationName + "] not found in search source for gap resolution");
        }
        return finalSource;
    }

    /**
     * Identifies gaps in Phase 2 results: terms that were reported by some shards
     * but not all shards. Returns a map from shard index (position in the activeResults
     * list) to the set of term keys that shard did NOT report.
     * <p>
     * Only shards in {@code respondedShards} are considered for gap resolution.
     * Shards that failed Phase 2 are excluded — sending Phase 3 requests to them
     * would likely fail again.
     *
     * @param phase2Responses Per-shard Phase 2 responses, indexed by shard position
     * @param aggregationName The name of the terms aggregation
     * @param numShards Total number of shards that participated in Phase 2
     * @param respondedShards Set of shard indices that successfully responded in Phase 2.
     *                        Only these shards will be considered for Phase 3 gap resolution.
     * @return Map from shard index to set of term keys needing gap resolution.
     *         Empty map if no gaps exist (all terms reported by all responding shards).
     */
    public static Map<Integer, List<String>> identifyGaps(
        Map<Integer, TermsRefinementShardResponse> phase2Responses,
        String aggregationName,
        int numShards,
        Set<Integer> respondedShards
    ) {
        // Step 1: For each shard, collect the set of term keys it reported
        Map<Integer, Set<String>> termsByShard = new HashMap<>();
        Set<String> allTermKeys = new HashSet<>();

        for (Map.Entry<Integer, TermsRefinementShardResponse> entry : phase2Responses.entrySet()) {
            int shardIdx = entry.getKey();
            TermsRefinementShardResponse response = entry.getValue();
            InternalAggregation termsAgg = response.aggregations().get(aggregationName);

            Set<String> shardTerms = new HashSet<>();
            if (termsAgg instanceof AbstractInternalTerms<?, ?> internalTerms) {
                for (var bucket : internalTerms.getBuckets()) {
                    String key = bucket.getKeyAsString();
                    shardTerms.add(key);
                    allTermKeys.add(key);
                }
            }
            termsByShard.put(shardIdx, shardTerms);
        }

        if (allTermKeys.isEmpty()) {
            return Map.of();
        }

        // Step 2: For each responding shard, find terms it didn't report (gaps).
        // Skip shards that failed Phase 2 — they won't respond to Phase 3 either.
        Map<Integer, List<String>> gapsByShard = new HashMap<>();
        for (int shardIdx = 0; shardIdx < numShards; shardIdx++) {
            if (respondedShards.contains(shardIdx) == false) {
                continue; // skip failed shards
            }
            Set<String> shardTerms = termsByShard.getOrDefault(shardIdx, Set.of());
            List<String> gaps = new ArrayList<>();
            for (String termKey : allTermKeys) {
                if (shardTerms.contains(termKey) == false) {
                    gaps.add(termKey);
                }
            }
            if (gaps.isEmpty() == false) {
                gapsByShard.put(shardIdx, gaps);
            }
        }

        return gapsByShard;
    }

    /**
     * Properly reduces Phase 2 (and optionally Phase 3) refinement results from all shards.
     * Each per-shard response is wrapped in its own InternalAggregations, then the standard
     * reduce merges same-key buckets across shards to produce exact counts.
     *
     * @param responses List of per-shard refinement results (Phase 2 + Phase 3)
     * @param aggregationName The name of the terms aggregation
     * @param reduceContext The reduce context for performing the final reduction
     * @return The properly reduced aggregation results with exact counts
     */
    public static InternalAggregations reduceRefinementResults(
        List<TermsRefinementShardResponse> responses,
        String aggregationName,
        AggregationReduceContext reduceContext
    ) {
        List<InternalAggregations> perShardAggs = new ArrayList<>();
        for (TermsRefinementShardResponse response : responses) {
            InternalAggregation termsAgg = response.aggregations().get(aggregationName);
            if (termsAgg != null) {
                perShardAggs.add(InternalAggregations.from(termsAgg));
            }
        }
        if (perShardAggs.isEmpty()) {
            return InternalAggregations.EMPTY;
        }
        if (perShardAggs.size() == 1) {
            return perShardAggs.get(0);
        }
        return InternalAggregations.reduce(perShardAggs, reduceContext);
    }
}
