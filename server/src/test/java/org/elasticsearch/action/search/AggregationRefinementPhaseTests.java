/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementCoordinator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementShardRequest;
import org.elasticsearch.search.aggregations.bucket.terms.TermsRefinementShardResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class AggregationRefinementPhaseTests extends ESTestCase {

    /**
     * When there are no refinement targets, the phase should proceed directly to fetch.
     */
    public void testEmptyTargetsGoesToFetch() {
        MockSearchPhaseContext mockContext = new MockSearchPhaseContext(1);
        try {
            SearchPhaseResults<SearchPhaseResult> results = createMockResults(1);
            SearchPhaseController.ReducedQueryPhase reducedPhase = createReducedPhase();

            AtomicInteger fetchCallCount = new AtomicInteger();
            AtomicReference<SearchPhaseController.ReducedQueryPhase> fetchPhase = new AtomicReference<>();

            AggregationRefinementPhase phase = new AggregationRefinementPhase(
                mockContext,
                results,
                reducedPhase,
                null,
                Collections.emptyList()
            ) {
                @Override
                void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
                    fetchCallCount.incrementAndGet();
                    fetchPhase.set(phase);
                }
            };

            assertEquals(AggregationRefinementPhase.NAME, phase.getName());
            phase.run();
            mockContext.assertNoFailure();

            // Should have called proceedToFetch exactly once
            assertThat(fetchCallCount.get(), equalTo(1));
            // Should have passed through the original reduced phase unchanged
            assertThat(fetchPhase.get(), notNullValue());
        } finally {
            mockContext.results.close();
        }
    }

    /**
     * With a single refinement target, the phase should send refinement requests
     * to all shards, collect responses, merge them, and proceed to fetch.
     */
    public void testSingleTargetRefinement() {
        MockSearchPhaseContext mockContext = new MockSearchPhaseContext(2);
        try {
            SearchPhaseResults<SearchPhaseResult> results = createMockResults(2);
            SearchPhaseController.ReducedQueryPhase reducedPhase = createReducedPhase();

            // Track which aggregation names were requested
            List<String> requestedAggNames = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger fetchCallCount = new AtomicInteger();

            // Mock transport to respond with refinement results
            mockContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                public void sendExecuteTermsRefinement(
                    Transport.Connection connection,
                    TermsRefinementShardRequest request,
                    SearchTask task,
                    ActionListener<TermsRefinementShardResponse> listener
                ) {
                    requestedAggNames.add(request.aggregationName());
                    StringTerms terms = createStringTerms(request.aggregationName(), List.of("red", "blue"), List.of(50, 30));
                    InternalAggregations aggs = InternalAggregations.from(List.of(terms));
                    listener.onResponse(new TermsRefinementShardResponse(request.shardId(), aggs, 2));
                }
            };

            TermsRefinementCoordinator.RefinementTarget target = new TermsRefinementCoordinator.RefinementTarget("my_terms", 10, 100);

            AggregationRefinementPhase phase = new AggregationRefinementPhase(
                mockContext,
                results,
                reducedPhase,
                null,
                List.of(target)
            ) {
                @Override
                void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
                    fetchCallCount.incrementAndGet();
                }
            };

            phase.run();
            mockContext.assertNoFailure();

            // Both shards should have received refinement requests for "my_terms"
            assertThat(requestedAggNames, hasSize(2));
            assertThat(requestedAggNames.get(0), equalTo("my_terms"));
            assertThat(requestedAggNames.get(1), equalTo("my_terms"));

            // Should have proceeded to fetch after refinement
            assertThat(fetchCallCount.get(), equalTo(1));
        } finally {
            mockContext.results.close();
        }
    }

    /**
     * With multiple refinement targets, the phase should process all targets
     * sequentially, sending refinement requests for each one.
     */
    public void testMultipleTargetsProcessedSequentially() {
        MockSearchPhaseContext mockContext = new MockSearchPhaseContext(2);
        try {
            SearchPhaseResults<SearchPhaseResult> results = createMockResults(2);
            SearchPhaseController.ReducedQueryPhase reducedPhase = createReducedPhase();

            // Track the order of aggregation names requested
            List<String> requestedAggNames = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger fetchCallCount = new AtomicInteger();

            mockContext.searchTransport = new SearchTransportService(null, null, null) {
                @Override
                public void sendExecuteTermsRefinement(
                    Transport.Connection connection,
                    TermsRefinementShardRequest request,
                    SearchTask task,
                    ActionListener<TermsRefinementShardResponse> listener
                ) {
                    requestedAggNames.add(request.aggregationName());
                    StringTerms terms = createStringTerms(
                        request.aggregationName(),
                        List.of("term1", "term2"),
                        List.of(100, 50)
                    );
                    InternalAggregations aggs = InternalAggregations.from(List.of(terms));
                    listener.onResponse(new TermsRefinementShardResponse(request.shardId(), aggs, 2));
                }
            };

            // Three refinement targets — should all be processed
            List<TermsRefinementCoordinator.RefinementTarget> targets = List.of(
                new TermsRefinementCoordinator.RefinementTarget("color_terms", 10, 100),
                new TermsRefinementCoordinator.RefinementTarget("brand_terms", 20, 100),
                new TermsRefinementCoordinator.RefinementTarget("size_terms", 5, 100)
            );

            AggregationRefinementPhase phase = new AggregationRefinementPhase(
                mockContext,
                results,
                reducedPhase,
                null,
                targets
            ) {
                @Override
                void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
                    fetchCallCount.incrementAndGet();
                }
            };

            phase.run();
            mockContext.assertNoFailure();

            // 3 targets × 2 shards = 6 refinement requests
            assertThat(requestedAggNames, hasSize(6));

            // Verify all three aggregation names appear (2 times each, once per shard)
            long colorCount = requestedAggNames.stream().filter(n -> n.equals("color_terms")).count();
            long brandCount = requestedAggNames.stream().filter(n -> n.equals("brand_terms")).count();
            long sizeCount = requestedAggNames.stream().filter(n -> n.equals("size_terms")).count();
            assertThat(colorCount, equalTo(2L));
            assertThat(brandCount, equalTo(2L));
            assertThat(sizeCount, equalTo(2L));

            // Verify sequential ordering: all color_terms requests come before brand_terms,
            // and all brand_terms come before size_terms
            int lastColorIdx = -1, firstBrandIdx = Integer.MAX_VALUE;
            int lastBrandIdx = -1, firstSizeIdx = Integer.MAX_VALUE;
            for (int i = 0; i < requestedAggNames.size(); i++) {
                switch (requestedAggNames.get(i)) {
                    case "color_terms" -> lastColorIdx = i;
                    case "brand_terms" -> {
                        firstBrandIdx = Math.min(firstBrandIdx, i);
                        lastBrandIdx = i;
                    }
                    case "size_terms" -> firstSizeIdx = Math.min(firstSizeIdx, i);
                }
            }
            assertTrue("color_terms should be processed before brand_terms", lastColorIdx < firstBrandIdx);
            assertTrue("brand_terms should be processed before size_terms", lastBrandIdx < firstSizeIdx);

            // Should have proceeded to fetch exactly once (after all targets)
            assertThat(fetchCallCount.get(), equalTo(1));
        } finally {
            mockContext.results.close();
        }
    }

    /**
     * When a shard fails during refinement, the phase should continue with
     * remaining shards and still proceed to fetch.
     */
    public void testShardFailureDuringRefinement() {
        MockSearchPhaseContext mockContext = new MockSearchPhaseContext(2);
        try {
            SearchPhaseResults<SearchPhaseResult> results = createMockResults(2);
            SearchPhaseController.ReducedQueryPhase reducedPhase = createReducedPhase();

            AtomicInteger fetchCallCount = new AtomicInteger();

            // First shard fails, second succeeds
            mockContext.searchTransport = new SearchTransportService(null, null, null) {
                private int callCount = 0;

                @Override
                public void sendExecuteTermsRefinement(
                    Transport.Connection connection,
                    TermsRefinementShardRequest request,
                    SearchTask task,
                    ActionListener<TermsRefinementShardResponse> listener
                ) {
                    callCount++;
                    if (callCount == 1) {
                        listener.onFailure(new RuntimeException("shard failed"));
                    } else {
                        StringTerms terms = createStringTerms(request.aggregationName(), List.of("red"), List.of(50));
                        InternalAggregations aggs = InternalAggregations.from(List.of(terms));
                        listener.onResponse(new TermsRefinementShardResponse(request.shardId(), aggs, 1));
                    }
                }
            };

            TermsRefinementCoordinator.RefinementTarget target = new TermsRefinementCoordinator.RefinementTarget("my_terms", 10, 100);

            AggregationRefinementPhase phase = new AggregationRefinementPhase(
                mockContext,
                results,
                reducedPhase,
                null,
                List.of(target)
            ) {
                @Override
                void proceedToFetch(SearchPhaseController.ReducedQueryPhase phase) {
                    fetchCallCount.incrementAndGet();
                }
            };

            phase.run();
            // Phase should complete without failure (shard failures are logged but tolerated)
            mockContext.assertNoFailure();

            // Should still proceed to fetch despite shard failure
            assertThat(fetchCallCount.get(), equalTo(1));
        } finally {
            mockContext.results.close();
        }
    }

    /**
     * Creates mock search phase results with the given number of shards.
     * Each shard result has a SearchShardTarget and a ShardSearchRequest with a source.
     */
    private SearchPhaseResults<SearchPhaseResult> createMockResults(int numShards) {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(numShards);
        for (int i = 0; i < numShards; i++) {
            QuerySearchResult queryResult = new QuerySearchResult();
            SearchShardTarget shardTarget = new SearchShardTarget("node" + i, new ShardId("index", "index", i), null);
            queryResult.setSearchShardTarget(shardTarget);
            queryResult.setShardIndex(i);
            queryResult.setShardSearchRequest(createShardSearchRequest(i));
            array.set(i, queryResult);
        }
        return new ArraySearchPhaseResults<>(numShards) {
            @Override
            AtomicArray<SearchPhaseResult> getAtomicArray() {
                return array;
            }
        };
    }

    /**
     * Creates a minimal ReducedQueryPhase for testing — no hits, no aggregations.
     */
    private SearchPhaseController.ReducedQueryPhase createReducedPhase() {
        return new SearchPhaseController.ReducedQueryPhase(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            0,
            Float.NaN,
            false,
            null,
            null,
            InternalAggregations.EMPTY,
            null,
            new SearchPhaseController.SortedTopDocs(new ScoreDoc[0], false, null, null, null, 0),
            new DocValueFormat[0],
            null,
            1,
            0,
            0,
            true,
            null
        );
    }

    private ShardSearchRequest createShardSearchRequest(int shardId) {
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "index" }, IndicesOptions.strictExpandOpen());
        SearchRequest searchRequest = new SearchRequest(originalIndices.indices());
        searchRequest.source(new SearchSourceBuilder().size(10));
        searchRequest.allowPartialSearchResults(true);
        return new ShardSearchRequest(
            originalIndices,
            searchRequest,
            new ShardId("index", "index", shardId),
            0,
            5,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null
        );
    }

    private static StringTerms createStringTerms(String name, List<String> termNames, List<Integer> docCounts) {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < termNames.size(); i++) {
            buckets.add(
                new StringTerms.Bucket(
                    new BytesRef(termNames.get(i)),
                    docCounts.get(i),
                    InternalAggregations.EMPTY,
                    false,
                    0L,
                    DocValueFormat.RAW
                )
            );
        }
        return new StringTerms(
            name,
            BucketOrder.count(false),
            BucketOrder.count(false),
            10,
            1,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            0,
            buckets,
            0L
        );
    }
}
