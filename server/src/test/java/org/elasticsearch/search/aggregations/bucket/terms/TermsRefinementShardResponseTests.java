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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TermsRefinementShardResponseTests extends ESTestCase {

    public void testSerializationWithEmptyAggregations() throws IOException {
        ShardId shardId = new ShardId("test-index", "_na_", 1);
        TermsRefinementShardResponse response = new TermsRefinementShardResponse(shardId, InternalAggregations.EMPTY, 0);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput in = new NamedWriteableAwareStreamInput(
            out.bytes().streamInput(),
            new NamedWriteableRegistry(Collections.emptyList())
        );
        TermsRefinementShardResponse deserialized = new TermsRefinementShardResponse(in);

        assertThat(deserialized.shardId(), equalTo(shardId));
        assertThat(deserialized.totalTermsAboveThreshold(), equalTo(0));
    }

    public void testSerializationRoundTripWithAggregations() throws IOException {
        ShardId shardId = new ShardId("test-index", "_na_", 0);
        StringTerms terms = createStringTerms("my_terms", List.of("red", "blue"), List.of(50, 30));
        InternalAggregations aggregations = InternalAggregations.from(List.of(terms));

        TermsRefinementShardResponse response = new TermsRefinementShardResponse(shardId, aggregations, 2);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        // Use SearchModule's named writeables for full deserialization
        NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(org.elasticsearch.common.settings.Settings.EMPTY, List.of()).getNamedWriteables());
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);

        TermsRefinementShardResponse deserialized = new TermsRefinementShardResponse(in);

        assertThat(deserialized.shardId(), equalTo(shardId));
        assertThat(deserialized.totalTermsAboveThreshold(), equalTo(2));
        assertThat(deserialized.aggregations(), notNullValue());
        assertThat(deserialized.aggregations().get("my_terms"), notNullValue());
    }

    public void testAccessors() {
        ShardId shardId = new ShardId("test-index", "_na_", 2);
        StringTerms terms = createStringTerms("color", List.of("green"), List.of(10));
        InternalAggregations aggregations = InternalAggregations.from(List.of(terms));

        TermsRefinementShardResponse response = new TermsRefinementShardResponse(shardId, aggregations, 1);

        assertThat(response.shardId(), equalTo(shardId));
        assertThat(response.totalTermsAboveThreshold(), equalTo(1));
        assertThat(response.aggregations(), notNullValue());
    }

    public void testGetTermsAggregation() {
        ShardId shardId = new ShardId("test-index", "_na_", 0);
        StringTerms terms = createStringTerms("my_terms", List.of("a", "b"), List.of(5, 3));
        InternalAggregations aggregations = InternalAggregations.from(List.of(terms));

        TermsRefinementShardResponse response = new TermsRefinementShardResponse(shardId, aggregations, 2);

        StringTerms retrieved = response.getTermsAggregation("my_terms");
        assertThat(retrieved, notNullValue());
        assertThat(retrieved.getName(), equalTo("my_terms"));
    }

    private StringTerms createStringTerms(String name, List<String> termNames, List<Integer> docCounts) {
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
