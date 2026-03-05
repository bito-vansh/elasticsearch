/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementCoordinator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementShardRequest;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementShardResponse;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementService;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TPUT Phase 2/3 aggregation refinement phase. Executes between the query
 * reduce and the fetch phase when the coordinator detects that a terms
 * aggregation with mode=EXACT needs refinement.
 * <p>
 * For each refinement target:
 * <ol>
 *   <li>Computes TPUT threshold T = τ₁ / m</li>
 *   <li>Sends threshold-filtered query to all shards</li>
 *   <li>Collects and merges results (now with exact counts)</li>
 *   <li>Replaces the provisional aggregation results</li>
 * </ol>
 * Then delegates to FetchSearchPhase.
 */
class AggregationRefinementPhase extends SearchPhase {
    static final String NAME = "aggregation_refinement";
    private static final Logger logger = LogManager.getLogger(AggregationRefinementPhase.class);

    private final AbstractSearchAsyncAction<?> context;
    private final SearchPhaseResults<SearchPhaseResult> queryResults;
    private final SearchPhaseController.ReducedQueryPhase reducedQueryPhase;
    private final AggregatedDfs aggregatedDfs;
    private final List<TermsRefinementCoordinator.RefinementTarget> targets;

    AggregationRefinementPhase(
        AbstractSearchAsyncAction<?> context,
        SearchPhaseResults<SearchPhaseResult> queryResults,
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
        AggregatedDfs aggregatedDfs,
        List<TermsRefinementCoordinator.RefinementTarget> targets
    ) {
        super(NAME);
        this.context = context;
        this.queryResults = queryResults;
        this.reducedQueryPhase = reducedQueryPhase;
        this.aggregatedDfs = aggregatedDfs;
        this.targets = targets;
    }

    @Override
    protected void run() {
        context.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                innerRun();
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(NAME, "TPUT refinement failed", e);
            }
        });
    }

    private void innerRun() {
        if (targets.isEmpty()) {
            proceedToFetch(reducedQueryPhase);
            return;
        }

        // Collect active shard results once — shared across all targets
        AtomicArray<SearchPhaseResult> shardResults = queryResults.getAtomicArray();
        List<SearchPhaseResult> activeResults = new ArrayList<>();
        for (SearchPhaseResult result : shardResults.asList()) {
            if (result != null && result.getSearchShardTarget() != null) {
                activeResults.add(result);
            }
        }

        if (activeResults.isEmpty()) {
            proceedToFetch(reducedQueryPhase);
            return;
        }

        // Process all refinement targets sequentially, threading the
        // ReducedQueryPhase through each refinement so that each target's
        // refined aggregations are visible to subsequent targets.
        refineTarget(0, reducedQueryPhase, activeResults);
    }

    /**
     * Refines the target at {@code targetIndex} and then chains to the next target.
     * When all targets have been processed, proceeds to the fetch phase.
     */
    private void refineTarget(
        int targetIndex,
        SearchPhaseController.ReducedQueryPhase currentPhase,
        List<SearchPhaseResult> activeResults
    ) {
        if (targetIndex >= targets.size()) {
            proceedToFetch(currentPhase);
            return;
        }

        TermsRefinementCoordinator.RefinementTarget target = targets.get(targetIndex);
        logger.debug(
            "Starting TPUT Phase 2 refinement for [{}] with threshold [{}] (target {}/{})",
            target.aggregationName(),
            target.threshold(),
            targetIndex + 1,
            targets.size()
        );

        // Send refinement requests to all shards
        List<TermsRefinementShardResponse> responses = Collections.synchronizedList(new ArrayList<>());
        // Track which shard indices (in activeResults) successfully responded in Phase 2,
        // so that Phase 3 doesn't send gap resolution requests to shards that already failed.
        Set<Integer> phase2SuccessShards = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger remaining = new AtomicInteger(activeResults.size());

        for (int idx = 0; idx < activeResults.size(); idx++) {
            final int shardIdx = idx;
            SearchPhaseResult queryResult = activeResults.get(idx);
            SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
            ShardSearchRequest shardRequest = queryResult.getShardSearchRequest();
            if (shardRequest == null || shardRequest.source() == null) {
                if (remaining.decrementAndGet() == 0) {
                    onAllShardsResponded(targetIndex, target, responses, phase2SuccessShards, currentPhase, activeResults);
                }
                continue;
            }

            TermsRefinementShardRequest refinementRequest = new TermsRefinementShardRequest(
                context.getOriginalIndices(queryResult.getShardIndex()),
                shardTarget.getShardId(),
                shardRequest,
                target.aggregationName(),
                target.threshold()
            );

            try {
                Transport.Connection connection = context.getConnection(
                    shardTarget.getClusterAlias(),
                    shardTarget.getNodeId()
                );
                context.getSearchTransport().sendExecuteTermsRefinement(
                    connection,
                    refinementRequest,
                    context.getTask(),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(TermsRefinementShardResponse response) {
                            responses.add(response);
                            phase2SuccessShards.add(shardIdx);
                            logger.debug(
                                "Received refinement response from shard [{}]: {} terms above threshold",
                                response.shardId(),
                                response.totalTermsAboveThreshold()
                            );
                            if (remaining.decrementAndGet() == 0) {
                                onAllShardsResponded(
                                    targetIndex, target, responses, phase2SuccessShards, currentPhase, activeResults
                                );
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug("Refinement request to shard [{}] failed", shardTarget.getShardId(), e);
                            // Continue despite shard failures — fall back to Phase 1 results
                            if (remaining.decrementAndGet() == 0) {
                                onAllShardsResponded(
                                    targetIndex, target, responses, phase2SuccessShards, currentPhase, activeResults
                                );
                            }
                        }
                    }
                );
            } catch (Exception e) {
                logger.debug("Failed to send refinement request to shard [{}]", shardTarget.getShardId(), e);
                if (remaining.decrementAndGet() == 0) {
                    onAllShardsResponded(targetIndex, target, responses, phase2SuccessShards, currentPhase, activeResults);
                }
            }
        }
    }

    private void onAllShardsResponded(
        int targetIndex,
        TermsRefinementCoordinator.RefinementTarget target,
        List<TermsRefinementShardResponse> responses,
        Set<Integer> phase2SuccessShards,
        SearchPhaseController.ReducedQueryPhase currentPhase,
        List<SearchPhaseResult> activeResults
    ) {
        try {
            if (responses.isEmpty()) {
                logger.debug("No Phase 2 responses received for [{}]; keeping Phase 1 results", target.aggregationName());
                refineTarget(targetIndex + 1, currentPhase, activeResults);
                return;
            }

            // Build a map from shard index to Phase 2 response for gap identification
            Map<Integer, TermsRefinementShardResponse> phase2ByShardIdx = new HashMap<>();
            for (TermsRefinementShardResponse response : responses) {
                // Map by shard index position within activeResults
                for (int i = 0; i < activeResults.size(); i++) {
                    SearchPhaseResult queryResult = activeResults.get(i);
                    if (queryResult.getSearchShardTarget() != null
                        && queryResult.getSearchShardTarget().getShardId().equals(response.shardId())) {
                        phase2ByShardIdx.put(i, response);
                        break;
                    }
                }
            }

            // Phase 3: Identify gaps — terms reported by some shards but not all.
            // Only consider shards that successfully responded in Phase 2; failed shards
            // are excluded so we don't send Phase 3 requests to shards that already failed.
            Map<Integer, List<String>> gapsByShard = TermsRefinementService.identifyGaps(
                phase2ByShardIdx,
                target.aggregationName(),
                activeResults.size(),
                phase2SuccessShards
            );

            if (gapsByShard.isEmpty()) {
                logger.debug("Phase 2 complete for [{}], no gaps detected — skipping Phase 3", target.aggregationName());
                finishRefinement(targetIndex, target, responses, currentPhase, activeResults);
                return;
            }

            logger.debug(
                "Phase 3 gap resolution for [{}]: {} shards have gaps",
                target.aggregationName(),
                gapsByShard.size()
            );

            // Send Phase 3 gap resolution requests to shards with gaps
            List<TermsRefinementShardResponse> phase3Responses = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger phase3Remaining = new AtomicInteger(gapsByShard.size());

            for (Map.Entry<Integer, List<String>> entry : gapsByShard.entrySet()) {
                int shardIdx = entry.getKey();
                List<String> gapTerms = entry.getValue();
                SearchPhaseResult queryResult = activeResults.get(shardIdx);
                SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
                ShardSearchRequest shardRequest = queryResult.getShardSearchRequest();

                if (shardRequest == null || shardRequest.source() == null) {
                    if (phase3Remaining.decrementAndGet() == 0) {
                        onPhase3Complete(targetIndex, target, responses, phase3Responses, currentPhase, activeResults);
                    }
                    continue;
                }

                TermsRefinementShardRequest gapRequest = new TermsRefinementShardRequest(
                    context.getOriginalIndices(queryResult.getShardIndex()),
                    shardTarget.getShardId(),
                    shardRequest,
                    target.aggregationName(),
                    1, // threshold not meaningful for Phase 3 (include filter controls selection)
                    gapTerms
                );

                try {
                    Transport.Connection connection = context.getConnection(
                        shardTarget.getClusterAlias(),
                        shardTarget.getNodeId()
                    );
                    context.getSearchTransport().sendExecuteTermsRefinement(
                        connection,
                        gapRequest,
                        context.getTask(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(TermsRefinementShardResponse response) {
                                phase3Responses.add(response);
                                logger.debug(
                                    "Phase 3 response from shard [{}]: {} terms resolved",
                                    response.shardId(),
                                    response.totalTermsAboveThreshold()
                                );
                                if (phase3Remaining.decrementAndGet() == 0) {
                                    onPhase3Complete(
                                        targetIndex, target, responses, phase3Responses, currentPhase, activeResults
                                    );
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.debug("Phase 3 request to shard [{}] failed", shardTarget.getShardId(), e);
                                if (phase3Remaining.decrementAndGet() == 0) {
                                    onPhase3Complete(
                                        targetIndex, target, responses, phase3Responses, currentPhase, activeResults
                                    );
                                }
                            }
                        }
                    );
                } catch (Exception e) {
                    logger.debug("Failed to send Phase 3 request to shard [{}]", shardTarget.getShardId(), e);
                    if (phase3Remaining.decrementAndGet() == 0) {
                        onPhase3Complete(targetIndex, target, responses, phase3Responses, currentPhase, activeResults);
                    }
                }
            }
        } catch (Exception e) {
            context.onPhaseFailure(NAME, "Failed Phase 2/3 for [" + target.aggregationName() + "]", e);
        }
    }

    private void onPhase3Complete(
        int targetIndex,
        TermsRefinementCoordinator.RefinementTarget target,
        List<TermsRefinementShardResponse> phase2Responses,
        List<TermsRefinementShardResponse> phase3Responses,
        SearchPhaseController.ReducedQueryPhase currentPhase,
        List<SearchPhaseResult> activeResults
    ) {
        try {
            logger.debug(
                "Phase 3 complete for [{}]: {} gap responses received",
                target.aggregationName(),
                phase3Responses.size()
            );
            // Combine Phase 2 + Phase 3 responses for final merge
            List<TermsRefinementShardResponse> allResponses = new ArrayList<>(phase2Responses.size() + phase3Responses.size());
            allResponses.addAll(phase2Responses);
            allResponses.addAll(phase3Responses);

            finishRefinement(targetIndex, target, allResponses, currentPhase, activeResults);
        } catch (Exception e) {
            context.onPhaseFailure(NAME, "Failed to merge Phase 3 results for [" + target.aggregationName() + "]", e);
        }
    }

    private void finishRefinement(
        int targetIndex,
        TermsRefinementCoordinator.RefinementTarget target,
        List<TermsRefinementShardResponse> allResponses,
        SearchPhaseController.ReducedQueryPhase currentPhase,
        List<SearchPhaseResult> activeResults
    ) {
        // Build a reduce context for proper merging of per-shard results
        var aggBuilders = context.getRequest().source() != null ? context.getRequest().source().aggregations() : null;
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            context.bigArrays,
            null, // ScriptService not needed for terms aggregation reduce
            () -> context.getTask().isCancelled(),
            aggBuilders,
            i -> {} // no-op multiBucketConsumer — result is bounded by Phase 2/3 collection
        );

        InternalAggregations refinedTermsAggs = TermsRefinementService.reduceRefinementResults(
            allResponses,
            target.aggregationName(),
            reduceContext
        );
        logger.debug("Refinement merge complete for [{}]", target.aggregationName());

        // Merge the refined terms aggregation back into the existing aggregations,
        // preserving all non-target aggregations (e.g. max, avg, other terms aggs).
        InternalAggregations mergedAggs = mergeRefinedAggregation(
            currentPhase.aggregations(),
            refinedTermsAggs,
            target.aggregationName()
        );

        // Create a new ReducedQueryPhase with the merged aggregations
        SearchPhaseController.ReducedQueryPhase refined = new SearchPhaseController.ReducedQueryPhase(
            currentPhase.totalHits(),
            currentPhase.fetchHits(),
            currentPhase.maxScore(),
            currentPhase.timedOut(),
            currentPhase.terminatedEarly(),
            currentPhase.suggest(),
            mergedAggs,
            currentPhase.profileBuilder(),
            currentPhase.sortedTopDocs(),
            currentPhase.sortValueFormats(),
            currentPhase.queryPhaseRankCoordinatorContext(),
            currentPhase.numReducePhases(),
            currentPhase.size(),
            currentPhase.from(),
            currentPhase.isEmptyResult(),
            currentPhase.timeRangeFilterFromMillis()
        );

        // Chain to the next target with the updated phase
        refineTarget(targetIndex + 1, refined, activeResults);
    }

    /**
     * Merges a refined terms aggregation back into the original aggregation set,
     * replacing only the target aggregation while preserving all others.
     */
    static InternalAggregations mergeRefinedAggregation(
        InternalAggregations original,
        InternalAggregations refinedTermsAggs,
        String targetAggregationName
    ) {
        if (original == null) {
            return refinedTermsAggs;
        }
        InternalAggregation refinedTermsAgg = refinedTermsAggs.get(targetAggregationName);
        if (refinedTermsAgg == null) {
            return original;
        }
        List<InternalAggregation> merged = new ArrayList<>();
        for (InternalAggregation agg : original.asList()) {
            if (agg.getName().equals(targetAggregationName)) {
                merged.add(refinedTermsAgg);
            } else {
                merged.add(agg);
            }
        }
        return InternalAggregations.from(merged);
    }

    // package-private for testing
    void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
        context.executeNextPhase(
            NAME,
            () -> new FetchSearchPhase(queryResults, aggregatedDfs, context, phase)
        );
    }
}
