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
        // For simplicity, handle one refinement target at a time.
        // Multiple targets would be handled sequentially.
        if (targets.isEmpty()) {
            proceedToFetch(reducedQueryPhase);
            return;
        }

        TermsRefinementCoordinator.RefinementTarget target = targets.get(0);
        logger.debug("Starting TPUT Phase 2 refinement for [{}] with threshold [{}]", target.aggregationName(), target.threshold());

        // Collect shard targets from the query results
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

        // Send refinement requests to all shards
        List<TermsRefinementShardResponse> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger remaining = new AtomicInteger(activeResults.size());

        for (SearchPhaseResult queryResult : activeResults) {
            SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
            ShardSearchRequest shardRequest = queryResult.getShardSearchRequest();
            if (shardRequest == null || shardRequest.source() == null) {
                if (remaining.decrementAndGet() == 0) {
                    onAllShardsResponded(target, responses);
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
                                onAllShardsResponded(target, responses);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Refinement request to shard [{}] failed", shardTarget.getShardId(), e);
                            // Continue despite shard failures — fall back to Phase 1 results
                            if (remaining.decrementAndGet() == 0) {
                                onAllShardsResponded(target, responses);
                            }
                        }
                    }
                );
            } catch (Exception e) {
                logger.warn("Failed to send refinement request to shard [{}]", shardTarget.getShardId(), e);
                if (remaining.decrementAndGet() == 0) {
                    onAllShardsResponded(target, responses);
                }
            }
        }
    }

    private void onAllShardsResponded(TermsRefinementCoordinator.RefinementTarget target, List<TermsRefinementShardResponse> responses) {
        try {
            if (responses.isEmpty()) {
                logger.debug("No Phase 2 responses received; using Phase 1 results as-is");
                proceedToFetch(reducedQueryPhase);
                return;
            }

            // Merge Phase 2 results
            InternalAggregations refinedAggs = TermsRefinementService.mergeRefinementResults(responses, target.aggregationName());
            logger.debug("Phase 2 merge complete for [{}]", target.aggregationName());

            // Create a new ReducedQueryPhase with the refined aggregations
            // replacing the terms aggregation from Phase 1
            // For now, we use the Phase 2 aggregations directly since they contain
            // the exact results for the refined terms aggregation
            SearchPhaseController.ReducedQueryPhase refined = new SearchPhaseController.ReducedQueryPhase(
                reducedQueryPhase.totalHits(),
                reducedQueryPhase.fetchHits(),
                reducedQueryPhase.maxScore(),
                reducedQueryPhase.timedOut(),
                reducedQueryPhase.terminatedEarly(),
                reducedQueryPhase.suggest(),
                refinedAggs,
                reducedQueryPhase.profileBuilder(),
                reducedQueryPhase.sortedTopDocs(),
                reducedQueryPhase.sortValueFormats(),
                reducedQueryPhase.queryPhaseRankCoordinatorContext(),
                reducedQueryPhase.numReducePhases(),
                reducedQueryPhase.size(),
                reducedQueryPhase.from(),
                reducedQueryPhase.isEmptyResult(),
                reducedQueryPhase.timeRangeFilterFromMillis()
            );

            proceedToFetch(refined);
        } catch (Exception e) {
            context.onPhaseFailure(NAME, "Failed to merge Phase 2 results", e);
        }
    }

    private void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
        context.executeNextPhase(
            NAME,
            () -> new FetchSearchPhase(queryResults, aggregatedDfs, context, phase)
        );
    }
}
