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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testMergeRefinementResultsEmpty() {
        InternalAggregations result = TermsRefinementService.mergeRefinementResults(Collections.emptyList(), "my_terms");
        assertThat(result, sameInstance(InternalAggregations.EMPTY));
    }

    public void testMergeRefinementResultsSingleShard() {
        StringTerms shardTerms = createStringTermsWithBuckets("my_terms", List.of("red", "blue"), List.of(50, 30));
        TermsRefinementShardResponse response = new TermsRefinementShardResponse(
            new ShardId("test", "_na_", 0),
            InternalAggregations.from(List.of(shardTerms)),
            2
        );

        InternalAggregations merged = TermsRefinementService.mergeRefinementResults(List.of(response), "my_terms");

        assertThat(merged, notNullValue());
        assertThat(merged.get("my_terms"), notNullValue());
    }

    public void testMergeRefinementResultsMultipleShards() {
        StringTerms shard1 = createStringTermsWithBuckets("my_terms", List.of("red", "blue"), List.of(50, 30));
        StringTerms shard2 = createStringTermsWithBuckets("my_terms", List.of("red", "green"), List.of(40, 20));

        List<TermsRefinementShardResponse> responses = new ArrayList<>();
        responses.add(new TermsRefinementShardResponse(new ShardId("test", "_na_", 0), InternalAggregations.from(List.of(shard1)), 2));
        responses.add(new TermsRefinementShardResponse(new ShardId("test", "_na_", 1), InternalAggregations.from(List.of(shard2)), 2));

        InternalAggregations merged = TermsRefinementService.mergeRefinementResults(responses, "my_terms");

        assertThat(merged, notNullValue());
        assertThat(merged.get("my_terms"), notNullValue());
    }

    public void testMergeRefinementResultsIgnoresMissingAggregation() {
        // Response with no matching aggregation name should be skipped
        TermsRefinementShardResponse response = new TermsRefinementShardResponse(
            new ShardId("test", "_na_", 0),
            InternalAggregations.EMPTY,
            0
        );

        InternalAggregations result = TermsRefinementService.mergeRefinementResults(List.of(response), "my_terms");
        assertThat(result, sameInstance(InternalAggregations.EMPTY));
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
