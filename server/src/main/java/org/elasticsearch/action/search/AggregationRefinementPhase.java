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
import java.util.List;
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
        AtomicInteger remaining = new AtomicInteger(activeResults.size());

        for (SearchPhaseResult queryResult : activeResults) {
            SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
            ShardSearchRequest shardRequest = queryResult.getShardSearchRequest();
            if (shardRequest == null || shardRequest.source() == null) {
                if (remaining.decrementAndGet() == 0) {
                    onAllShardsResponded(targetIndex, target, responses, currentPhase, activeResults);
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
                            logger.debug(
                                "Received refinement response from shard [{}]: {} terms above threshold",
                                response.shardId(),
                                response.totalTermsAboveThreshold()
                            );
                            if (remaining.decrementAndGet() == 0) {
                                onAllShardsResponded(targetIndex, target, responses, currentPhase, activeResults);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug("Refinement request to shard [{}] failed", shardTarget.getShardId(), e);
                            // Continue despite shard failures — fall back to Phase 1 results
                            if (remaining.decrementAndGet() == 0) {
                                onAllShardsResponded(targetIndex, target, responses, currentPhase, activeResults);
                            }
                        }
                    }
                );
            } catch (Exception e) {
                logger.debug("Failed to send refinement request to shard [{}]", shardTarget.getShardId(), e);
                if (remaining.decrementAndGet() == 0) {
                    onAllShardsResponded(targetIndex, target, responses, currentPhase, activeResults);
                }
            }
        }
    }

    private void onAllShardsResponded(
        int targetIndex,
        TermsRefinementCoordinator.RefinementTarget target,
        List<TermsRefinementShardResponse> responses,
        SearchPhaseController.ReducedQueryPhase currentPhase,
        List<SearchPhaseResult> activeResults
    ) {
        try {
            if (responses.isEmpty()) {
                logger.debug("No Phase 2 responses received for [{}]; keeping Phase 1 results", target.aggregationName());
                // Move on to the next target with the current phase unchanged
                refineTarget(targetIndex + 1, currentPhase, activeResults);
                return;
            }

            // Merge Phase 2 results
            InternalAggregations refinedAggs = TermsRefinementService.mergeRefinementResults(responses, target.aggregationName());
            logger.debug("Phase 2 merge complete for [{}]", target.aggregationName());

            // Create a new ReducedQueryPhase with the refined aggregations
            SearchPhaseController.ReducedQueryPhase refined = new SearchPhaseController.ReducedQueryPhase(
                currentPhase.totalHits(),
                currentPhase.fetchHits(),
                currentPhase.maxScore(),
                currentPhase.timedOut(),
                currentPhase.terminatedEarly(),
                currentPhase.suggest(),
                refinedAggs,
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
        } catch (Exception e) {
            context.onPhaseFailure(NAME, "Failed to merge Phase 2 results for [" + target.aggregationName() + "]", e);
        }
    }

    // package-private for testing
    void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
        context.executeNextPhase(
            NAME,
            () -> new FetchSearchPhase(queryResults, aggregatedDfs, context, phase)
        );
    }
}
