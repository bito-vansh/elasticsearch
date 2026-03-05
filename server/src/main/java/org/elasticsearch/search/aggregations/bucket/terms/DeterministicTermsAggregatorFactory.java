/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.NumericTermsAggregator.ResultStrategy;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.pickSubAggColectMode;

/**
 * Factory for deterministic terms aggregators. Uses the TPUT approach where each shard
 * collects all unique terms (up to max_terms_per_shard) to ensure exact results during reduction.
 *
 * The key difference from standard TermsAggregatorFactory is that shard_size is set to
 * max_terms_per_shard (defaulting to Integer.MAX_VALUE) so each shard returns all its terms
 * rather than just an approximated top-K.
 */
public class DeterministicTermsAggregatorFactory extends ValuesSourceAggregatorFactory {

    private static final Logger logger = LogManager.getLogger(DeterministicTermsAggregatorFactory.class);

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            DeterministicTermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.KEYWORD, CoreValuesSourceType.IP),
            DeterministicTermsAggregatorFactory.bytesSupplier(),
            true
        );

        builder.register(
            DeterministicTermsAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            DeterministicTermsAggregatorFactory.numericSupplier(),
            true
        );
    }

    private static TermsAggregatorSupplier bytesSupplier() {
        return (
            name,
            factories,
            valuesSourceConfig,
            order,
            bucketCountThresholds,
            includeExclude,
            executionHint,
            context,
            parent,
            subAggCollectMode,
            showTermDocCountError,
            cardinality,
            metadata,
            excludeDeletedDocs) -> {
            ValuesSource valuesSource = valuesSourceConfig.getValuesSource();
            TermsAggregatorFactory.ExecutionMode execution = null;
            if (executionHint != null) {
                execution = TermsAggregatorFactory.ExecutionMode.fromString(executionHint);
            }
            if (valuesSource.hasOrdinals() == false) {
                execution = TermsAggregatorFactory.ExecutionMode.MAP;
            }
            if (execution == null) {
                execution = TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS;
            }
            final long maxOrd = execution == TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS
                ? getMaxOrd(valuesSource, context.searcher())
                : -1;
            if (subAggCollectMode == null) {
                subAggCollectMode = pickSubAggColectMode(factories, bucketCountThresholds.getShardSize(), maxOrd);
            }

            if ((includeExclude != null) && (includeExclude.isRegexBased()) && valuesSourceConfig.format() != DocValueFormat.RAW) {
                throw new IllegalArgumentException(
                    "Aggregation ["
                        + name
                        + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                        + "include/exclude clauses"
                );
            }

            logger.debug("Creating deterministic bytes terms aggregator with execution mode [{}]", execution);
            return execution.create(
                name,
                factories,
                valuesSourceConfig,
                order,
                bucketCountThresholds,
                includeExclude,
                context,
                parent,
                subAggCollectMode,
                showTermDocCountError,
                cardinality,
                metadata,
                excludeDeletedDocs
            );
        };
    }

    private static TermsAggregatorSupplier numericSupplier() {
        return (
            name,
            factories,
            valuesSourceConfig,
            order,
            bucketCountThresholds,
            includeExclude,
            executionHint,
            context,
            parent,
            subAggCollectMode,
            showTermDocCountError,
            cardinality,
            metadata,
            excludeDeletedDocs) -> {

            if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                throw new IllegalArgumentException(
                    "Aggregation ["
                        + name
                        + "] cannot support regular expression style "
                        + "include/exclude settings as they can only be applied to string fields. Use an array of numeric values for "
                        + "include/exclude clauses used to filter numeric fields"
                );
            }

            if (subAggCollectMode == null) {
                subAggCollectMode = pickSubAggColectMode(factories, bucketCountThresholds.getShardSize(), -1);
            }

            ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
            IncludeExclude.LongFilter longFilter = null;
            Function<NumericTermsAggregator, ResultStrategy<?, ?>> resultStrategy;
            if (numericValuesSource.isFloatingPoint()) {
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToDoubleFilter();
                }
                resultStrategy = agg -> agg.new DoubleTermsResults(showTermDocCountError, agg);
            } else {
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(valuesSourceConfig.format());
                }
                resultStrategy = agg -> agg.new LongTermsResults(showTermDocCountError, agg);
            }
            return new NumericTermsAggregator(
                name,
                factories,
                resultStrategy,
                numericValuesSource,
                valuesSourceConfig.format(),
                order,
                bucketCountThresholds,
                context,
                parent,
                subAggCollectMode,
                longFilter,
                cardinality,
                metadata,
                excludeDeletedDocs
            );
        };
    }

    private final TermsAggregatorSupplier aggregatorSupplier;
    private final BucketOrder order;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final SubAggCollectionMode collectMode;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final boolean showTermDocCountError;
    private final int maxTermsPerShard;

    DeterministicTermsAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        BucketOrder order,
        IncludeExclude includeExclude,
        String executionHint,
        SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        boolean showTermDocCountError,
        int maxTermsPerShard,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        TermsAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.order = order;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.collectMode = collectMode;
        this.bucketCountThresholds = bucketCountThresholds;
        this.showTermDocCountError = showTermDocCountError;
        this.maxTermsPerShard = maxTermsPerShard;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedTerms(
            name,
            order,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            metadata
        );
        Aggregator agg = new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
        order.validate(agg);
        return agg;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {

        // TPUT key insight: override shard_size to collect all terms per shard.
        // For deterministic results, each shard must return ALL its unique terms
        // (up to max_terms_per_shard) so the coordinator can compute exact global counts.
        // When ordered by key, the standard merge-sort approach already gives exact results,
        // so we only need the override for count-based ordering.
        BucketCountThresholds adjusted = new BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false) {
            adjusted.setShardSize(maxTermsPerShard);
        } else if (adjusted.getShardSize() == DeterministicTermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.shardSize()) {
            // For key-order, use the standard heuristic if no explicit shard_size
            adjusted.setShardSize(BucketUtils.suggestShardSideQueueSize(adjusted.getRequiredSize()));
        }
        adjusted.ensureValidity();

        return aggregatorSupplier.build(
            name,
            factories,
            config,
            order,
            adjusted,
            includeExclude,
            executionHint,
            context,
            parent,
            collectMode,
            showTermDocCountError,
            cardinality,
            metadata,
            false
        );
    }

    private static long getMaxOrd(ValuesSource source, org.apache.lucene.search.IndexSearcher searcher) throws IOException {
        if (source instanceof ValuesSource.Bytes.WithOrdinals valueSourceWithOrdinals) {
            return valueSourceWithOrdinals.globalMaxOrd(searcher.getIndexReader());
        } else {
            return -1;
        }
    }
}
