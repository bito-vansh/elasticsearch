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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class TermsRefinementServiceTests extends ESTestCase {

    public void testCreateRefinementSourceSetsMinDocCount() {
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.query(new MatchAllQueryBuilder());
        original.size(10);
        original.aggregation(new TermsAggregationBuilder("my_terms").field("color").size(5).mode(TermsAggregationMode.EXACT));

        SearchSourceBuilder refined = TermsRefinementService.createRefinementSource(original, "my_terms", 20);

        assertThat(refined.size(), equalTo(0)); // hits disabled
        assertThat(refined.aggregations(), notNullValue());

        AggregationBuilder refinedAgg = refined.aggregations().getAggregatorFactories().iterator().next();
        assertThat(refinedAgg, instanceOf(TermsAggregationBuilder.class));

        TermsAggregationBuilder refinedTerms = (TermsAggregationBuilder) refinedAgg;
        assertThat(refinedTerms.getName(), equalTo("my_terms"));
        assertThat(refinedTerms.minDocCount(), equalTo(20L));
        assertThat(refinedTerms.size(), equalTo(Integer.MAX_VALUE));
        assertThat(refinedTerms.shardSize(), equalTo(Integer.MAX_VALUE));
        assertThat(refinedTerms.mode(), equalTo(TermsAggregationMode.APPROXIMATE)); // prevents recursive refinement
    }

    public void testCreateRefinementSourceOnlyIncludesTargetAggregation() {
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.aggregation(new TermsAggregationBuilder("my_terms").field("color"));
        original.aggregation(new MaxAggregationBuilder("max_price").field("price"));

        SearchSourceBuilder refined = TermsRefinementService.createRefinementSource(original, "my_terms", 10);

        // Only the target terms aggregation should be included
        int count = 0;
        for (AggregationBuilder agg : refined.aggregations().getAggregatorFactories()) {
            assertThat(agg.getName(), equalTo("my_terms"));
            count++;
        }
        assertThat(count, equalTo(1));
    }

    public void testCreateRefinementSourceThrowsWhenNoAggregations() {
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.query(new MatchAllQueryBuilder());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TermsRefinementService.createRefinementSource(original, "my_terms", 10)
        );
        assertThat(e.getMessage(), equalTo("No aggregations in search source for refinement of [my_terms]"));
    }

    public void testCreateRefinementSourceThrowsWhenAggNotFound() {
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.aggregation(new MaxAggregationBuilder("max_price").field("price"));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TermsRefinementService.createRefinementSource(original, "missing_terms", 10)
        );
        assertThat(e.getMessage(), equalTo("Terms aggregation [missing_terms] not found in search source"));
    }

    public void testCreateRefinementSourcePreservesQuery() {
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.query(query);
        original.aggregation(new TermsAggregationBuilder("my_terms").field("color"));

        SearchSourceBuilder refined = TermsRefinementService.createRefinementSource(original, "my_terms", 10);

        assertThat(refined.query(), notNullValue());
        assertThat(refined.query(), instanceOf(MatchAllQueryBuilder.class));
    }

    // --- Phase 3: Gap Identification Tests ---

    public void testIdentifyGapsNoGaps() {
        // All shards report the same terms → no gaps
        StringTerms shard0 = createStringTermsWithBuckets("my_terms", List.of("red", "blue"), List.of(50, 30));
        StringTerms shard1 = createStringTermsWithBuckets("my_terms", List.of("red", "blue"), List.of(40, 20));

        Map<Integer, TermsRefinementShardResponse> phase2 = Map.of(
            0, new TermsRefinementShardResponse(new ShardId("test", "_na_", 0), InternalAggregations.from(List.of(shard0)), 2),
            1, new TermsRefinementShardResponse(new ShardId("test", "_na_", 1), InternalAggregations.from(List.of(shard1)), 2)
        );

        Map<Integer, List<String>> gaps = TermsRefinementService.identifyGaps(phase2, "my_terms", 2, Set.of(0, 1));
        assertTrue(gaps.isEmpty());
    }

    public void testIdentifyGapsWithGaps() {
        // Shard 0 has {red, blue}, shard 1 has {red, green} → shard 0 missing green, shard 1 missing blue
        StringTerms shard0 = createStringTermsWithBuckets("my_terms", List.of("red", "blue"), List.of(50, 30));
        StringTerms shard1 = createStringTermsWithBuckets("my_terms", List.of("red", "green"), List.of(40, 20));

        Map<Integer, TermsRefinementShardResponse> phase2 = Map.of(
            0, new TermsRefinementShardResponse(new ShardId("test", "_na_", 0), InternalAggregations.from(List.of(shard0)), 2),
            1, new TermsRefinementShardResponse(new ShardId("test", "_na_", 1), InternalAggregations.from(List.of(shard1)), 2)
        );

        Map<Integer, List<String>> gaps = TermsRefinementService.identifyGaps(phase2, "my_terms", 2, Set.of(0, 1));

        assertThat(gaps.size(), equalTo(2));
        assertThat(gaps.get(0), containsInAnyOrder("green"));
        assertThat(gaps.get(1), containsInAnyOrder("blue"));
    }

    public void testIdentifyGapsSkipsFailedShards() {
        // 3 shards: shard 0 and 1 responded, shard 2 failed Phase 2.
        // Shard 2 should NOT appear in gaps even though it's missing all terms.
        StringTerms shard0 = createStringTermsWithBuckets("my_terms", List.of("red", "blue"), List.of(50, 30));
        StringTerms shard1 = createStringTermsWithBuckets("my_terms", List.of("red"), List.of(40));

        Map<Integer, TermsRefinementShardResponse> phase2 = Map.of(
            0, new TermsRefinementShardResponse(new ShardId("test", "_na_", 0), InternalAggregations.from(List.of(shard0)), 2),
            1, new TermsRefinementShardResponse(new ShardId("test", "_na_", 1), InternalAggregations.from(List.of(shard1)), 1)
        );

        // Only shards 0 and 1 responded; shard 2 failed
        Map<Integer, List<String>> gaps = TermsRefinementService.identifyGaps(phase2, "my_terms", 3, Set.of(0, 1));

        // Shard 1 is missing "blue", but shard 2 (failed) should NOT be in the gaps map
        assertThat(gaps.size(), equalTo(1));
        assertThat(gaps.get(1), containsInAnyOrder("blue"));
        assertNull(gaps.get(2));
    }

    public void testIdentifyGapsEmptyResponses() {
        Map<Integer, List<String>> gaps = TermsRefinementService.identifyGaps(Map.of(), "my_terms", 2, Set.of());
        assertTrue(gaps.isEmpty());
    }

    // --- Phase 3: Gap Resolution Source Tests ---

    public void testCreateGapResolutionSourceSetsIncludeFilter() {
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.query(new MatchAllQueryBuilder());
        original.aggregation(new TermsAggregationBuilder("my_terms").field("color").size(5).mode(TermsAggregationMode.EXACT));

        List<String> termsToResolve = List.of("green", "yellow");
        SearchSourceBuilder gapSource = TermsRefinementService.createGapResolutionSource(original, "my_terms", termsToResolve);

        assertThat(gapSource.size(), equalTo(0));
        assertThat(gapSource.aggregations(), notNullValue());

        AggregationBuilder gapAgg = gapSource.aggregations().getAggregatorFactories().iterator().next();
        assertThat(gapAgg, instanceOf(TermsAggregationBuilder.class));

        TermsAggregationBuilder gapTerms = (TermsAggregationBuilder) gapAgg;
        assertThat(gapTerms.getName(), equalTo("my_terms"));
        assertThat(gapTerms.minDocCount(), equalTo(1L));
        assertThat(gapTerms.size(), equalTo(Integer.MAX_VALUE));
        assertThat(gapTerms.shardSize(), equalTo(Integer.MAX_VALUE));
        assertThat(gapTerms.mode(), equalTo(TermsAggregationMode.APPROXIMATE));
        assertThat(gapTerms.includeExclude(), notNullValue()); // include filter is set
    }

    public void testCreateGapResolutionSourcePreservesQuery() {
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        SearchSourceBuilder original = new SearchSourceBuilder();
        original.query(query);
        original.aggregation(new TermsAggregationBuilder("my_terms").field("color"));

        SearchSourceBuilder gapSource = TermsRefinementService.createGapResolutionSource(original, "my_terms", List.of("red"));

        assertThat(gapSource.query(), notNullValue());
        assertThat(gapSource.query(), instanceOf(MatchAllQueryBuilder.class));
    }

    public void testCreateGapResolutionSourceThrowsWhenNoAggregations() {
        SearchSourceBuilder original = new SearchSourceBuilder();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TermsRefinementService.createGapResolutionSource(original, "my_terms", List.of("red"))
        );
        assertThat(e.getMessage(), equalTo("No aggregations in search source for gap resolution of [my_terms]"));
    }

    private StringTerms createStringTermsWithBuckets(String name, List<String> termNames, List<Integer> docCounts) {
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < termNames.size(); i++) {
            buckets.add(
                new StringTerms.Bucket(
                    new BytesRef(termNames.get(i)),
                    docCounts.get(i),
                    InternalAggregations.EMPTY,
                    false,
                    0,
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
