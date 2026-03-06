/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation builder for deterministic terms aggregation using the TPUT (Two-Phase Uniform Threshold) algorithm.
 * <p>
 * Unlike the standard terms aggregation which is approximate in distributed settings, this aggregation
 * guarantees exact top-K results by using a two-phase approach:
 * <ol>
 *   <li><b>Phase 1</b>: Each shard collects all terms (up to max_terms_per_shard) with their counts.</li>
 *   <li><b>Phase 2 (reduce)</b>: The coordinator merges results from all shards with exact counts,
 *       then selects the true global top-K terms. Since all shards report all their terms,
 *       there is no approximation error (doc_count_error_upper_bound is always 0).</li>
 * </ol>
 * <p>
 * This is more expensive than the standard terms aggregation in terms of network and memory, but
 * produces deterministic, exact results. Use when accuracy is more important than performance.
 */
public class DeterministicTermsAggregationBuilder extends ValuesSourceAggregationBuilder<DeterministicTermsAggregationBuilder> {

    public static final String NAME = "deterministic_terms";

    private static final TransportVersion DETERMINISTIC_TERMS_AGG_VERSION = TransportVersion.fromName("deterministic_terms_agg");

    public static final ValuesSourceRegistry.RegistryKey<TermsAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        TermsAggregatorSupplier.class
    );

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField MAX_TERMS_PER_SHARD_FIELD_NAME = new ParseField("max_terms_per_shard");

    static final TermsAggregator.ConstantBucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS =
        new TermsAggregator.ConstantBucketCountThresholds(1, 0, 10, -1);

    /**
     * Default maximum number of unique terms to collect per shard.
     * Set to Integer.MAX_VALUE to collect all terms (exact mode).
     * Can be lowered for memory/performance tradeoff at the cost of potentially incomplete results.
     */
    public static final int DEFAULT_MAX_TERMS_PER_SHARD = Integer.MAX_VALUE;

    public static final ObjectParser<DeterministicTermsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        DeterministicTermsAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareBoolean(DeterministicTermsAggregationBuilder::showTermDocCountError, SHOW_TERM_DOC_COUNT_ERROR);
        PARSER.declareInt(DeterministicTermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);
        PARSER.declareLong(DeterministicTermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);
        PARSER.declareLong(DeterministicTermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);
        PARSER.declareInt(DeterministicTermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);
        PARSER.declareString(DeterministicTermsAggregationBuilder::executionHint, EXECUTION_HINT_FIELD_NAME);
        PARSER.declareInt(DeterministicTermsAggregationBuilder::maxTermsPerShard, MAX_TERMS_PER_SHARD_FIELD_NAME);

        PARSER.declareField(
            DeterministicTermsAggregationBuilder::collectMode,
            (p, c) -> SubAggCollectionMode.parse(p.text(), LoggingDeprecationHandler.INSTANCE),
            SubAggCollectionMode.KEY,
            ObjectParser.ValueType.STRING
        );

        PARSER.declareObjectArray(
            DeterministicTermsAggregationBuilder::order,
            (p, c) -> InternalOrder.Parser.parseOrderParam(p),
            ORDER_FIELD
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
            IncludeExclude::parseInclude,
            IncludeExclude.INCLUDE_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
            IncludeExclude::parseExclude,
            IncludeExclude.EXCLUDE_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        DeterministicTermsAggregatorFactory.registerAggregators(builder);
    }

    private BucketOrder order = BucketOrder.compound(BucketOrder.count(false));
    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private SubAggCollectionMode collectMode = null;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private boolean showTermDocCountError = false;
    private int maxTermsPerShard = DEFAULT_MAX_TERMS_PER_SHARD;

    public DeterministicTermsAggregationBuilder(String name) {
        super(name);
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }

    protected DeterministicTermsAggregationBuilder(
        DeterministicTermsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.order = clone.order;
        this.executionHint = clone.executionHint;
        this.includeExclude = clone.includeExclude;
        this.collectMode = clone.collectMode;
        this.bucketCountThresholds = new BucketCountThresholds(clone.bucketCountThresholds);
        this.showTermDocCountError = clone.showTermDocCountError;
        this.maxTermsPerShard = clone.maxTermsPerShard;
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new DeterministicTermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    public DeterministicTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        bucketCountThresholds = new BucketCountThresholds(in);
        collectMode = in.readOptionalWriteable(SubAggCollectionMode::readFromStream);
        executionHint = in.readOptionalString();
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        order = InternalOrder.Streams.readOrder(in);
        showTermDocCountError = in.readBoolean();
        maxTermsPerShard = in.readVInt();
    }

    @Override
    protected boolean serializeTargetValueType(TransportVersion version) {
        return true;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeOptionalWriteable(collectMode);
        out.writeOptionalString(executionHint);
        out.writeOptionalWriteable(includeExclude);
        order.writeTo(out);
        out.writeBoolean(showTermDocCountError);
        out.writeVInt(maxTermsPerShard);
    }

    public DeterministicTermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    public int size() {
        return bucketCountThresholds.getRequiredSize();
    }

    public DeterministicTermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    public int shardSize() {
        return bucketCountThresholds.getShardSize();
    }

    public DeterministicTermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    public long minDocCount() {
        return bucketCountThresholds.getMinDocCount();
    }

    public DeterministicTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    public long shardMinDocCount() {
        return bucketCountThresholds.getShardMinDocCount();
    }

    public DeterministicTermsAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if (order instanceof CompoundOrder || InternalOrder.isKeyOrder(order)) {
            this.order = order;
        } else {
            this.order = BucketOrder.compound(order);
        }
        return this;
    }

    public DeterministicTermsAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }

    public BucketOrder order() {
        return order;
    }

    public DeterministicTermsAggregationBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    public String executionHint() {
        return executionHint;
    }

    public DeterministicTermsAggregationBuilder collectMode(SubAggCollectionMode collectMode) {
        if (collectMode == null) {
            throw new IllegalArgumentException("[collectMode] must not be null: [" + name + "]");
        }
        this.collectMode = collectMode;
        return this;
    }

    public SubAggCollectionMode collectMode() {
        return collectMode;
    }

    public DeterministicTermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    public boolean showTermDocCountError() {
        return showTermDocCountError;
    }

    public DeterministicTermsAggregationBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    /**
     * Set the maximum number of unique terms to collect per shard.
     * Default is Integer.MAX_VALUE (collect all terms for exact results).
     * Lowering this trades accuracy for memory/performance.
     */
    public DeterministicTermsAggregationBuilder maxTermsPerShard(int maxTermsPerShard) {
        if (maxTermsPerShard <= 0) {
            throw new IllegalArgumentException(
                "[max_terms_per_shard] must be greater than 0. Found [" + maxTermsPerShard + "] in [" + name + "]"
            );
        }
        this.maxTermsPerShard = maxTermsPerShard;
        return this;
    }

    public int maxTermsPerShard() {
        return maxTermsPerShard;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        TermsAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new DeterministicTermsAggregatorFactory(
            name,
            config,
            order,
            includeExclude,
            executionHint,
            collectMode,
            bucketCountThresholds,
            showTermDocCountError,
            maxTermsPerShard,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        if (maxTermsPerShard != DEFAULT_MAX_TERMS_PER_SHARD) {
            builder.field(MAX_TERMS_PER_SHARD_FIELD_NAME.getPreferredName(), maxTermsPerShard);
        }
        if (executionHint != null) {
            builder.field(EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        if (collectMode != null) {
            builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            bucketCountThresholds,
            collectMode,
            executionHint,
            includeExclude,
            order,
            showTermDocCountError,
            maxTermsPerShard
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        DeterministicTermsAggregationBuilder other = (DeterministicTermsAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
            && Objects.equals(collectMode, other.collectMode)
            && Objects.equals(executionHint, other.executionHint)
            && Objects.equals(includeExclude, other.includeExclude)
            && Objects.equals(order, other.order)
            && Objects.equals(showTermDocCountError, other.showTermDocCountError)
            && Objects.equals(maxTermsPerShard, other.maxTermsPerShard);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return DETERMINISTIC_TERMS_AGG_VERSION;
    }
}
