/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.List;

/**
 * TPUT Phase 2/3 shard request. Sent from the coordinator to each shard
 * to collect terms with doc_count >= threshold (Phase 2), or specific terms
 * that need gap resolution (Phase 3).
 * <p>
 * The shard re-executes the terms aggregation with a modified min_doc_count
 * equal to the threshold, returning all qualifying terms with their full
 * sub-aggregations.
 */
public class TermsRefinementShardRequest extends AbstractTransportRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardId shardId;
    private final ShardSearchRequest shardSearchRequest;
    private final String aggregationName;
    private final long threshold;
    private final List<String> termsToResolve;

    public TermsRefinementShardRequest(
        OriginalIndices originalIndices,
        ShardId shardId,
        ShardSearchRequest shardSearchRequest,
        String aggregationName,
        long threshold
    ) {
        this(originalIndices, shardId, shardSearchRequest, aggregationName, threshold, null);
    }

    public TermsRefinementShardRequest(
        OriginalIndices originalIndices,
        ShardId shardId,
        ShardSearchRequest shardSearchRequest,
        String aggregationName,
        long threshold,
        List<String> termsToResolve
    ) {
        this.originalIndices = originalIndices;
        this.shardId = shardId;
        this.shardSearchRequest = shardSearchRequest;
        this.aggregationName = aggregationName;
        this.threshold = threshold;
        this.termsToResolve = termsToResolve;
    }

    public TermsRefinementShardRequest(StreamInput in) throws IOException {
        super(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shardId = new ShardId(in);
        shardSearchRequest = new ShardSearchRequest(in);
        aggregationName = in.readString();
        threshold = in.readVLong();
        termsToResolve = in.readOptionalCollectionAsList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        shardId.writeTo(out);
        shardSearchRequest.writeTo(out);
        out.writeString(aggregationName);
        out.writeVLong(threshold);
        out.writeOptionalCollection(termsToResolve, StreamOutput::writeString);
    }

    public ShardId shardId() {
        return shardId;
    }

    public ShardSearchRequest shardSearchRequest() {
        return shardSearchRequest;
    }

    public String aggregationName() {
        return aggregationName;
    }

    /**
     * The TPUT threshold T = τ₁ / m. Shards should return all terms
     * with doc_count >= this threshold.
     */
    public long threshold() {
        return threshold;
    }

    /**
     * For Phase 3 gap resolution: the specific term keys to query on this shard.
     * When non-null, the shard should use an include filter to only collect these terms.
     * When null, this is a Phase 2 request (collect all terms above threshold).
     */
    public List<String> termsToResolve() {
        return termsToResolve;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }
}
