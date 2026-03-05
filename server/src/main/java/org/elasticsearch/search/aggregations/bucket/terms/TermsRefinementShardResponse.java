/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * TPUT Phase 2/3 shard response. Contains the terms above the threshold
 * from this shard, with their full sub-aggregations.
 */
public class TermsRefinementShardResponse extends TransportResponse {

    private final ShardId shardId;
    private final InternalAggregations aggregations;
    private final int totalTermsAboveThreshold;

    public TermsRefinementShardResponse(ShardId shardId, InternalAggregations aggregations, int totalTermsAboveThreshold) {
        this.shardId = shardId;
        this.aggregations = aggregations;
        this.totalTermsAboveThreshold = totalTermsAboveThreshold;
    }

    public TermsRefinementShardResponse(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        aggregations = InternalAggregations.readFrom(in);
        totalTermsAboveThreshold = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        aggregations.writeTo(out);
        out.writeVInt(totalTermsAboveThreshold);
    }

    public ShardId shardId() {
        return shardId;
    }

    /**
     * Returns the aggregation results from this shard,
     * containing all terms with doc_count >= threshold.
     */
    public InternalAggregations aggregations() {
        return aggregations;
    }

    /**
     * Returns the refined terms aggregation from this shard, if present.
     */
    @SuppressWarnings("unchecked")
    public <T extends InternalAggregation> T getTermsAggregation(String name) {
        return (T) aggregations.get(name);
    }

    public int totalTermsAboveThreshold() {
        return totalTermsAboveThreshold;
    }
}
