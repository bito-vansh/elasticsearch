/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TermsRefinementShardRequestTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()
    );

    public void testSerializationRoundTrip() throws IOException {
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "test-index" }, IndicesOptions.strictExpandOpen());
        ShardId shardId = new ShardId("test-index", "_na_", 0);

        ShardSearchRequest shardSearchRequest = createShardSearchRequest(originalIndices, shardId);

        String aggregationName = "my_terms_agg";
        long threshold = 42;

        TermsRefinementShardRequest request = new TermsRefinementShardRequest(
            originalIndices,
            shardId,
            shardSearchRequest,
            aggregationName,
            threshold
        );

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);

        TermsRefinementShardRequest deserialized = new TermsRefinementShardRequest(in);

        assertThat(deserialized.shardId(), equalTo(shardId));
        assertThat(deserialized.aggregationName(), equalTo(aggregationName));
        assertThat(deserialized.threshold(), equalTo(threshold));
        assertThat(deserialized.indices(), equalTo(new String[] { "test-index" }));
    }

    public void testIndicesRequest() {
        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { "index-a", "index-b" },
            IndicesOptions.strictExpandOpen()
        );
        ShardId shardId = new ShardId("index-a", "_na_", 0);

        ShardSearchRequest shardSearchRequest = createShardSearchRequest(originalIndices, shardId);

        TermsRefinementShardRequest request = new TermsRefinementShardRequest(
            originalIndices,
            shardId,
            shardSearchRequest,
            "test_agg",
            10
        );

        assertThat(request.indices(), equalTo(new String[] { "index-a", "index-b" }));
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.strictExpandOpen()));
    }

    public void testAccessors() {
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "test" }, IndicesOptions.strictExpandOpen());
        ShardId shardId = new ShardId("test", "_na_", 3);

        ShardSearchRequest shardSearchRequest = createShardSearchRequest(originalIndices, shardId);

        TermsRefinementShardRequest request = new TermsRefinementShardRequest(
            originalIndices,
            shardId,
            shardSearchRequest,
            "color_terms",
            99
        );

        assertThat(request.shardId(), equalTo(shardId));
        assertThat(request.aggregationName(), equalTo("color_terms"));
        assertThat(request.threshold(), equalTo(99L));
        assertNotNull(request.shardSearchRequest());
    }

    private ShardSearchRequest createShardSearchRequest(OriginalIndices originalIndices, ShardId shardId) {
        SearchRequest searchRequest = new SearchRequest(originalIndices.indices());
        searchRequest.source(new SearchSourceBuilder().size(10));
        searchRequest.allowPartialSearchResults(true);
        return new ShardSearchRequest(
            originalIndices, searchRequest, shardId, 0, 5, AliasFilter.EMPTY, 1.0f, System.currentTimeMillis(), null
        );
    }
}
