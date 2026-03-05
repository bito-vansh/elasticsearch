/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the deterministic terms aggregation (TPUT).
 * Verifies that results are exact with zero doc_count_error.
 */
public class DeterministicTermsAggregatorTests extends AggregatorTestCase {

    private static final String KEYWORD_FIELD = "keyword";
    private static final String LONG_FIELD = "long_value";

    /**
     * Basic test: verify deterministic terms returns correct top-K with exact counts.
     */
    public void testKeywordFieldTopK() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, true, true, Collections.emptyMap());

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // Create dataset: term "a" appears 5 times, "b" 3 times, "c" 1 time
                for (int i = 0; i < 5; i++) {
                    addKeywordDoc(indexWriter, "a");
                }
                for (int i = 0; i < 3; i++) {
                    addKeywordDoc(indexWriter, "b");
                }
                addKeywordDoc(indexWriter, "c");
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(KEYWORD_FIELD);
                builder.size(10);

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, keywordFieldType)
                );

                assertEquals(3, result.getBuckets().size());
                // Ordered by count descending
                assertEquals("a", result.getBuckets().get(0).getKeyAsString());
                assertEquals(5L, result.getBuckets().get(0).getDocCount());
                assertEquals("b", result.getBuckets().get(1).getKeyAsString());
                assertEquals(3L, result.getBuckets().get(1).getDocCount());
                assertEquals("c", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1L, result.getBuckets().get(2).getDocCount());
            }
        }
    }

    /**
     * Test that doc_count_error_upper_bound is 0 for deterministic terms.
     */
    public void testZeroDocCountError() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, true, true, Collections.emptyMap());

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 100; i++) {
                    addKeywordDoc(indexWriter, "term_" + (i % 20));
                }
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(KEYWORD_FIELD);
                builder.size(5);
                builder.showTermDocCountError(true);

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, keywordFieldType)
                );

                assertNotNull(result.getDocCountError());
                // With deterministic mode and single shard, error should be 0
                assertThat(result.getDocCountError(), equalTo(0L));
            }
        }
    }

    /**
     * Test with numeric (long) field.
     */
    public void testLongField() throws IOException {
        MappedFieldType longFieldType = new NumberFieldMapper.NumberFieldType(LONG_FIELD, NumberFieldMapper.NumberType.LONG);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 10; i++) {
                    addLongDoc(indexWriter, 100);
                }
                for (int i = 0; i < 5; i++) {
                    addLongDoc(indexWriter, 200);
                }
                for (int i = 0; i < 2; i++) {
                    addLongDoc(indexWriter, 300);
                }
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(LONG_FIELD);
                builder.size(10);

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, longFieldType)
                );

                assertEquals(3, result.getBuckets().size());
                assertEquals(100L, result.getBuckets().get(0).getKey());
                assertEquals(10L, result.getBuckets().get(0).getDocCount());
                assertEquals(200L, result.getBuckets().get(1).getKey());
                assertEquals(5L, result.getBuckets().get(1).getDocCount());
                assertEquals(300L, result.getBuckets().get(2).getKey());
                assertEquals(2L, result.getBuckets().get(2).getDocCount());
            }
        }
    }

    /**
     * Test with key ordering instead of count ordering.
     */
    public void testKeyOrder() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, true, true, Collections.emptyMap());

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 3; i++) addKeywordDoc(indexWriter, "c");
                for (int i = 0; i < 5; i++) addKeywordDoc(indexWriter, "a");
                for (int i = 0; i < 1; i++) addKeywordDoc(indexWriter, "b");
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(KEYWORD_FIELD);
                builder.size(10);
                builder.order(BucketOrder.key(true));

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, keywordFieldType)
                );

                assertEquals(3, result.getBuckets().size());
                assertEquals("a", result.getBuckets().get(0).getKeyAsString());
                assertEquals("b", result.getBuckets().get(1).getKeyAsString());
                assertEquals("c", result.getBuckets().get(2).getKeyAsString());
            }
        }
    }

    /**
     * Test with min_doc_count filtering.
     */
    public void testMinDocCount() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, true, true, Collections.emptyMap());

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 5; i++) addKeywordDoc(indexWriter, "frequent");
                addKeywordDoc(indexWriter, "rare");
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(KEYWORD_FIELD);
                builder.size(10);
                builder.minDocCount(2);

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, keywordFieldType)
                );

                assertEquals(1, result.getBuckets().size());
                assertEquals("frequent", result.getBuckets().get(0).getKeyAsString());
                assertEquals(5L, result.getBuckets().get(0).getDocCount());
            }
        }
    }

    /**
     * Test with max_terms_per_shard parameter.
     */
    public void testMaxTermsPerShard() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, true, true, Collections.emptyMap());

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 50; i++) {
                    addKeywordDoc(indexWriter, "term_" + i);
                }
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(KEYWORD_FIELD);
                builder.size(5);
                builder.maxTermsPerShard(100);

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, keywordFieldType)
                );

                // Should return 5 buckets (limited by size)
                assertEquals(5, result.getBuckets().size());
            }
        }
    }

    /**
     * Test empty result when no documents match.
     */
    public void testEmptyResult() throws IOException {
        MappedFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD, true, true, Collections.emptyMap());

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                // Add no documents
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test_agg");
                builder.field(KEYWORD_FIELD);
                builder.size(10);

                InternalMappedTerms<?, ?> result = searchAndReduce(
                    indexReader,
                    new AggTestConfig(builder, keywordFieldType)
                );

                assertEquals(0, result.getBuckets().size());
            }
        }
    }

    /**
     * Test builder serialization round-trip.
     */
    public void testBuilderSerialization() {
        DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test");
        builder.field(KEYWORD_FIELD);
        builder.size(20);
        builder.minDocCount(5);
        builder.maxTermsPerShard(10000);
        builder.showTermDocCountError(true);

        assertEquals("deterministic_terms", builder.getType());
        assertEquals(20, builder.size());
        assertEquals(5, builder.minDocCount());
        assertEquals(10000, builder.maxTermsPerShard());
        assertTrue(builder.showTermDocCountError());
    }

    /**
     * Test that invalid parameters are rejected.
     */
    public void testInvalidParameters() {
        DeterministicTermsAggregationBuilder builder = new DeterministicTermsAggregationBuilder("test");

        expectThrows(IllegalArgumentException.class, () -> builder.size(0));
        expectThrows(IllegalArgumentException.class, () -> builder.size(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.shardSize(0));
        expectThrows(IllegalArgumentException.class, () -> builder.minDocCount(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.shardMinDocCount(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.maxTermsPerShard(0));
        expectThrows(IllegalArgumentException.class, () -> builder.maxTermsPerShard(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.order((BucketOrder) null));
        expectThrows(IllegalArgumentException.class, () -> builder.collectMode(null));
    }

    private void addKeywordDoc(RandomIndexWriter indexWriter, String value) throws IOException {
        Document document = new Document();
        document.add(new SortedDocValuesField(KEYWORD_FIELD, new BytesRef(value)));
        FieldType fieldType = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
        fieldType.freeze();
        document.add(new Field(KEYWORD_FIELD, new BytesRef(value), fieldType));
        indexWriter.addDocument(document);
    }

    private void addLongDoc(RandomIndexWriter indexWriter, long value) throws IOException {
        Document document = new Document();
        document.add(new NumericDocValuesField(LONG_FIELD, value));
        document.add(new LongPoint(LONG_FIELD, value));
        indexWriter.addDocument(document);
    }
}
