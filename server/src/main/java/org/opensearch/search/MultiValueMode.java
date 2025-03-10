/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.common.Numbers;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.fielddata.AbstractBinaryDocValues;
import org.opensearch.index.fielddata.AbstractNumericDocValues;
import org.opensearch.index.fielddata.AbstractSortedDocValues;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.LongToSortedNumericUnsignedLongValues;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericUnsignedLongValues;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum MultiValueMode implements Writeable {
    /**
     * Pick the sum of all the values.
     */
    SUM {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total;
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }

                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            return totalCount > 0 ? totalValue : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            final int count = values.docValueCount();
            double total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total;
        }

        @Override
        protected double pick(
            SortedNumericDoubleValues values,
            double missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            double totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }

            return totalCount > 0 ? totalValue : missingValue;
        }

        @Override
        protected long pick(SortedNumericUnsignedLongValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total;
        }

        @Override
        protected long pick(
            SortedNumericUnsignedLongValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }

                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            return totalCount > 0 ? totalValue : missingValue;
        }
    },

    /**
     * Pick the average of all the values.
     */
    AVG {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return count > 1 ? Math.round((double) total / (double) count) : total;
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            if (totalCount < 1) {
                return missingValue;
            }
            return totalCount > 1 ? Math.round((double) totalValue / (double) totalCount) : totalValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            final int count = values.docValueCount();
            double total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total / count;
        }

        @Override
        protected double pick(
            SortedNumericDoubleValues values,
            double missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            double totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            if (totalCount < 1) {
                return missingValue;
            }
            return totalValue / totalCount;
        }

        @Override
        protected long pick(SortedNumericUnsignedLongValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return count > 1 ? divideUnsignedAndRoundUp(total, count) : total;
        }

        @Override
        protected long pick(
            SortedNumericUnsignedLongValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            if (totalCount < 1) {
                return missingValue;
            }
            return totalCount > 1 ? divideUnsignedAndRoundUp(totalValue, totalCount) : totalValue;
        }
    },

    /**
     * Pick the median of the values.
     */
    MEDIAN {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            int count = values.docValueCount();
            for (int i = 0; i < (count - 1) / 2; ++i) {
                values.nextValue();
            }
            if (count % 2 == 0) {
                return Math.round(((double) values.nextValue() + values.nextValue()) / 2);
            } else {
                return values.nextValue();
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            int count = values.docValueCount();
            for (int i = 0; i < (count - 1) / 2; ++i) {
                values.nextValue();
            }
            if (count % 2 == 0) {
                return (values.nextValue() + values.nextValue()) / 2;
            } else {
                return values.nextValue();
            }
        }

        @Override
        protected long pick(SortedNumericUnsignedLongValues values) throws IOException {
            int count = values.docValueCount();
            long firstValue = values.nextValue();
            if (count == 1) {
                return firstValue;
            } else if (count == 2) {
                long total = firstValue + values.nextValue();
                return (total >>> 1) + (total & 1);
            } else if (firstValue >= 0) {
                for (int i = 1; i < (count - 1) / 2; ++i) {
                    values.nextValue();
                }
                if (count % 2 == 0) {
                    long total = values.nextValue() + values.nextValue();
                    return (total >>> 1) + (total & 1);
                } else {
                    return values.nextValue();
                }
            }

            final long[] docValues = new long[count];
            docValues[0] = firstValue;
            int firstPositiveIndex = 0;
            for (int i = 1; i < count; ++i) {
                docValues[i] = values.nextValue();
                if (docValues[i] >= 0 && firstPositiveIndex == 0) {
                    firstPositiveIndex = i;
                }
            }
            final int mid = ((count - 1) / 2 + firstPositiveIndex) % count;
            if (count % 2 == 0) {
                long total = docValues[mid] + docValues[(mid + 1) % count];
                return (total >>> 1) + (total & 1);
            } else {
                return docValues[mid];
            }
        }
    },

    /**
     * Pick the lowest value.
     */
    MIN {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            return values.nextValue();
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            long minValue = Long.MAX_VALUE;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    minValue = Math.min(minValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? minValue : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            return values.nextValue();
        }

        @Override
        protected double pick(
            SortedNumericDoubleValues values,
            double missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            double minValue = Double.POSITIVE_INFINITY;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    minValue = Math.min(minValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? minValue : missingValue;
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values) throws IOException {
            return values.nextValue();
        }

        @Override
        protected BytesRef pick(
            BinaryDocValues values,
            BytesRefBuilder builder,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            BytesRefBuilder bytesRefBuilder = null;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final BytesRef innerValue = values.binaryValue();
                    if (bytesRefBuilder == null) {
                        builder.copyBytes(innerValue);
                        bytesRefBuilder = builder;
                    } else {
                        final BytesRef min = bytesRefBuilder.get().compareTo(innerValue) <= 0 ? bytesRefBuilder.get() : innerValue;
                        if (min == innerValue) {
                            bytesRefBuilder.copyBytes(min);
                        }
                    }
                }
            }
            return bytesRefBuilder == null ? null : bytesRefBuilder.get();
        }

        @Override
        protected int pick(SortedSetDocValues values) throws IOException {
            return Math.toIntExact(values.nextOrd());
        }

        @Override
        protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int ord = Integer.MAX_VALUE;
            boolean hasValue = false;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int innerOrd = values.ordValue();
                    ord = Math.min(ord, innerOrd);
                    hasValue = true;
                }
            }

            return hasValue ? ord : -1;
        }

        @Override
        protected long pick(SortedNumericUnsignedLongValues values) throws IOException {
            final int count = values.docValueCount();
            final long min = values.nextValue();
            if (count == 1 || min > 0) {
                return min;
            }
            for (int i = 1; i < count; ++i) {
                long val = values.nextValue();
                if (val >= 0) {
                    return val;
                }
            }
            return min;
        }

        @Override
        protected long pick(
            SortedNumericUnsignedLongValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            long minValue = Numbers.MAX_UNSIGNED_LONG_VALUE_AS_LONG;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final long docMin = pick(values);
                    minValue = Long.compareUnsigned(docMin, minValue) < 0 ? docMin : minValue;
                    hasValue = true;
                }
            }
            return hasValue ? minValue : missingValue;
        }
    },

    /**
     * Pick the highest value.
     */
    MAX {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            for (int i = 0; i < count - 1; ++i) {
                values.nextValue();
            }
            return values.nextValue();
        }

        @Override
        protected long pick(
            SortedNumericDocValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            long maxValue = Long.MIN_VALUE;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int i = 0; i < docCount - 1; ++i) {
                        values.nextValue();
                    }
                    maxValue = Math.max(maxValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? maxValue : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            final int count = values.docValueCount();
            for (int i = 0; i < count - 1; ++i) {
                values.nextValue();
            }
            return values.nextValue();
        }

        @Override
        protected double pick(
            SortedNumericDoubleValues values,
            double missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            double maxValue = Double.NEGATIVE_INFINITY;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int i = 0; i < docCount - 1; ++i) {
                        values.nextValue();
                    }
                    maxValue = Math.max(maxValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? maxValue : missingValue;
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values) throws IOException {
            int count = values.docValueCount();
            for (int i = 0; i < count - 1; ++i) {
                values.nextValue();
            }
            return values.nextValue();
        }

        @Override
        protected BytesRef pick(
            BinaryDocValues values,
            BytesRefBuilder builder,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            BytesRefBuilder bytesRefBuilder = null;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final BytesRef innerValue = values.binaryValue();
                    if (bytesRefBuilder == null) {
                        builder.copyBytes(innerValue);
                        bytesRefBuilder = builder;
                    } else {
                        final BytesRef max = bytesRefBuilder.get().compareTo(innerValue) > 0 ? bytesRefBuilder.get() : innerValue;
                        if (max == innerValue) {
                            bytesRefBuilder.copyBytes(max);
                        }
                    }
                }
            }
            return bytesRefBuilder == null ? null : bytesRefBuilder.get();
        }

        @Override
        protected int pick(SortedSetDocValues values) throws IOException {
            long maxOrd = -1;
            int count = values.docValueCount();
            long ord;
            while ((count-- > 0) && (ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                maxOrd = ord;
            }
            return Math.toIntExact(maxOrd);
        }

        @Override
        protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int ord = -1;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    ord = Math.max(ord, values.ordValue());
                }
            }
            return ord;
        }

        @Override
        protected long pick(SortedNumericUnsignedLongValues values) throws IOException {
            final int count = values.docValueCount();
            long max = values.nextValue();
            long val;
            for (int i = 1; i < count; ++i) {
                val = values.nextValue();
                if (max < 0 && val >= 0) {
                    return max;
                }
                max = val;
            }
            return max;
        }

        @Override
        protected long pick(
            SortedNumericUnsignedLongValues values,
            long missingValue,
            DocIdSetIterator docItr,
            int startDoc,
            int endDoc,
            int maxChildren
        ) throws IOException {
            boolean hasValue = false;
            long maxValue = Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final long docMax = pick(values);
                    maxValue = Long.compareUnsigned(maxValue, docMax) < 0 ? docMax : maxValue;
                    hasValue = true;
                }
            }
            return hasValue ? maxValue : missingValue;
        }
    };

    /**
     * A case insensitive version of {@link #valueOf(String)}
     *
     * @throws IllegalArgumentException if the given string doesn't match a sort mode or is <code>null</code>.
     */
    public static MultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal sort mode: " + sortMode);
        }
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MEDIAN, MIN, MAX
     */
    public NumericDocValues select(final SortedNumericDocValues values) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return singleton;
        } else {
            return new AbstractNumericDocValues() {

                private long value;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (values.advanceExact(target)) {
                        value = pick(values);
                        return true;
                    }
                    return false;
                }

                @Override
                public int docID() {
                    return values.docID();
                }

                @Override
                public long longValue() throws IOException {
                    return value;
                }
            };
        }
    }

    protected long pick(SortedNumericDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     * <p>
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MIN, MAX
     * <p>
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public NumericDocValues select(
        final SortedNumericDocValues values,
        final long missingValue,
        final BitSet parentDocs,
        final DocIdSetIterator childDocs,
        int maxDoc,
        int maxChildren
    ) throws IOException {
        if (parentDocs == null || childDocs == null) {
            return FieldData.replaceMissing(DocValues.emptyNumeric(), missingValue);
        }

        return new AbstractNumericDocValues() {

            int lastSeenParentDoc = -1;
            long lastEmittedValue = missingValue;

            @Override
            public boolean advanceExact(int parentDoc) throws IOException {
                assert parentDoc >= lastSeenParentDoc : "can only evaluate current and upcoming parent docs";
                if (parentDoc == lastSeenParentDoc) {
                    return true;
                } else if (parentDoc == 0) {
                    lastEmittedValue = missingValue;
                    return true;
                }
                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                final int firstChildDoc;
                if (childDocs.docID() > prevParentDoc) {
                    firstChildDoc = childDocs.docID();
                } else {
                    firstChildDoc = childDocs.advance(prevParentDoc + 1);
                }

                lastSeenParentDoc = parentDoc;
                lastEmittedValue = pick(values, missingValue, childDocs, firstChildDoc, parentDoc, maxChildren);
                return true;
            }

            @Override
            public int docID() {
                return lastSeenParentDoc;
            }

            @Override
            public long longValue() {
                return lastEmittedValue;
            }
        };
    }

    protected long pick(
        SortedNumericDocValues values,
        long missingValue,
        DocIdSetIterator docItr,
        int startDoc,
        int endDoc,
        int maxChildren
    ) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MEDIAN, MIN, MAX
     */
    public NumericDoubleValues select(final SortedNumericDoubleValues values) {
        final NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            return singleton;
        } else {
            return new NumericDoubleValues() {

                private double value;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (values.advanceExact(target)) {
                        value = pick(values);
                        return true;
                    }
                    return false;
                }

                @Override
                public double doubleValue() throws IOException {
                    return this.value;
                }

                @Override
                public int advance(int target) throws IOException {
                    return values.advance(target);
                }
            };
        }
    }

    protected double pick(SortedNumericDoubleValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     * <p>
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MIN, MAX
     * <p>
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public NumericDoubleValues select(
        final SortedNumericDoubleValues values,
        final double missingValue,
        final BitSet parentDocs,
        final DocIdSetIterator childDocs,
        int maxDoc,
        int maxChildren
    ) throws IOException {
        if (parentDocs == null || childDocs == null) {
            return FieldData.replaceMissing(FieldData.emptyNumericDouble(), missingValue);
        }

        return new NumericDoubleValues() {

            int lastSeenParentDoc = 0;
            double lastEmittedValue = missingValue;

            @Override
            public boolean advanceExact(int parentDoc) throws IOException {
                assert parentDoc >= lastSeenParentDoc : "can only evaluate current and upcoming parent docs";
                if (parentDoc == lastSeenParentDoc) {
                    return true;
                }
                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                final int firstChildDoc;
                if (childDocs.docID() > prevParentDoc) {
                    firstChildDoc = childDocs.docID();
                } else {
                    firstChildDoc = childDocs.advance(prevParentDoc + 1);
                }

                lastSeenParentDoc = parentDoc;
                lastEmittedValue = pick(values, missingValue, childDocs, firstChildDoc, parentDoc, maxChildren);
                return true;
            }

            @Override
            public double doubleValue() throws IOException {
                return lastEmittedValue;
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }
        };
    }

    protected double pick(
        SortedNumericDoubleValues values,
        double missingValue,
        DocIdSetIterator docItr,
        int startDoc,
        int endDoc,
        int maxChildren
    ) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link BinaryDocValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: MIN, MAX
     */
    public BinaryDocValues select(final SortedBinaryDocValues values, final BytesRef missingValue) {
        final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            if (missingValue == null) {
                return singleton;
            }
            return new AbstractBinaryDocValues() {

                private BytesRef value;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    this.value = singleton.advanceExact(target) ? singleton.binaryValue() : missingValue;
                    return true;
                }

                @Override
                public BytesRef binaryValue() throws IOException {
                    return this.value;
                }
            };
        } else {
            return new AbstractBinaryDocValues() {

                private BytesRef value;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (values.advanceExact(target)) {
                        value = pick(values);
                        return true;
                    } else {
                        value = missingValue;
                        return missingValue != null;
                    }
                }

                @Override
                public BytesRef binaryValue() throws IOException {
                    return value;
                }
            };
        }
    }

    protected BytesRef pick(SortedBinaryDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link BinaryDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     * <p>
     * For every root document, the values of its inner documents will be aggregated.
     * If none of the inner documents has a value, then <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: MIN, MAX
     * <p>
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public BinaryDocValues select(
        final SortedBinaryDocValues values,
        final BytesRef missingValue,
        final BitSet parentDocs,
        final DocIdSetIterator childDocs,
        int maxDoc,
        int maxChildren
    ) throws IOException {
        if (parentDocs == null || childDocs == null) {
            return select(FieldData.emptySortedBinary(), missingValue);
        }
        final BinaryDocValues selectedValues = select(values, null);

        return new AbstractBinaryDocValues() {

            final BytesRefBuilder builder = new BytesRefBuilder();

            int lastSeenParentDoc = 0;
            BytesRef lastEmittedValue = missingValue;

            @Override
            public boolean advanceExact(int parentDoc) throws IOException {
                assert parentDoc >= lastSeenParentDoc : "can only evaluate current and upcoming root docs";
                if (parentDoc == lastSeenParentDoc) {
                    return true;
                }

                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                final int firstChildDoc;
                if (childDocs.docID() > prevParentDoc) {
                    firstChildDoc = childDocs.docID();
                } else {
                    firstChildDoc = childDocs.advance(prevParentDoc + 1);
                }

                lastSeenParentDoc = parentDoc;
                lastEmittedValue = pick(selectedValues, builder, childDocs, firstChildDoc, parentDoc, maxChildren);
                if (lastEmittedValue == null) {
                    lastEmittedValue = missingValue;
                }
                return true;
            }

            @Override
            public BytesRef binaryValue() throws IOException {
                return lastEmittedValue;
            }
        };
    }

    protected BytesRef pick(
        BinaryDocValues values,
        BytesRefBuilder builder,
        DocIdSetIterator docItr,
        int startDoc,
        int endDoc,
        int maxChildren
    ) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort documents
     * with this mode and the provided values.
     * <p>
     * Allowed Modes: MIN, MAX
     */
    public SortedDocValues select(final SortedSetDocValues values) {
        if (values.getValueCount() >= Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                "fields containing more than " + (Integer.MAX_VALUE - 1) + " unique terms are unsupported"
            );
        }

        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return singleton;
        } else {
            return new AbstractSortedDocValues() {

                int ord;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (values.advanceExact(target)) {
                        ord = pick(values);
                        return true;
                    } else {
                        ord = -1;
                        return false;
                    }
                }

                @Override
                public int docID() {
                    return values.docID();
                }

                @Override
                public int ordValue() {
                    assert ord != -1;
                    return ord;
                }

                @Override
                public BytesRef lookupOrd(int ord) throws IOException {
                    return values.lookupOrd(ord);
                }

                @Override
                public int getValueCount() {
                    return (int) values.getValueCount();
                }
            };
        }
    }

    protected int pick(SortedSetDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     * <p>
     * For every root document, the values of its inner documents will be aggregated.
     * <p>
     * Allowed Modes: MIN, MAX
     * <p>
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public SortedDocValues select(
        final SortedSetDocValues values,
        final BitSet parentDocs,
        final DocIdSetIterator childDocs,
        int maxChildren
    ) throws IOException {
        if (parentDocs == null || childDocs == null) {
            return select(DocValues.emptySortedSet());
        }
        final SortedDocValues selectedValues = select(values);

        return new AbstractSortedDocValues() {

            int docID = -1;
            int lastSeenParentDoc = 0;
            int lastEmittedOrd = -1;

            @Override
            public BytesRef lookupOrd(int ord) throws IOException {
                return selectedValues.lookupOrd(ord);
            }

            @Override
            public int getValueCount() {
                return selectedValues.getValueCount();
            }

            @Override
            public boolean advanceExact(int parentDoc) throws IOException {
                assert parentDoc >= lastSeenParentDoc : "can only evaluate current and upcoming root docs";
                if (parentDoc == lastSeenParentDoc) {
                    return lastEmittedOrd != -1;
                }

                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                final int firstChildDoc;
                if (childDocs.docID() > prevParentDoc) {
                    firstChildDoc = childDocs.docID();
                } else {
                    firstChildDoc = childDocs.advance(prevParentDoc + 1);
                }

                docID = lastSeenParentDoc = parentDoc;
                lastEmittedOrd = pick(selectedValues, childDocs, firstChildDoc, parentDoc, maxChildren);
                return lastEmittedOrd != -1;
            }

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int ordValue() {
                return lastEmittedOrd;
            }
        };
    }

    protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link NumericDoubleValues} instance that can be used to sort documents
     * with this mode and the provided values. When a document has no value,
     * <code>missingValue</code> is returned.
     * <p>
     * Allowed Modes: SUM, AVG, MEDIAN, MIN, MAX
     */
    public NumericDocValues select(final SortedNumericUnsignedLongValues values) {
        SortedNumericDocValues sortedNumericDocValues = null;
        if (values instanceof LongToSortedNumericUnsignedLongValues) {
            sortedNumericDocValues = ((LongToSortedNumericUnsignedLongValues) values).getNumericUnsignedLongValues();
        }

        final NumericDocValues singleton = DocValues.unwrapSingleton(sortedNumericDocValues);
        if (singleton != null) {
            return singleton;
        } else {
            return new AbstractNumericDocValues() {

                private long value;

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (values.advanceExact(target)) {
                        value = pick(values);
                        return true;
                    }
                    return false;
                }

                @Override
                public int docID() {
                    return values.docID();
                }

                @Override
                public long longValue() throws IOException {
                    return value;
                }
            };
        }
    }

    protected long pick(SortedNumericUnsignedLongValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    /**
     * Return a {@link SortedDocValues} instance that can be used to sort root documents
     * with this mode, the provided values and filters for root/inner documents.
     * <p>
     * For every root document, the values of its inner documents will be aggregated.
     * <p>
     * Allowed Modes: MIN, MAX
     * <p>
     * NOTE: Calling the returned instance on docs that are not root docs is illegal
     *       The returned instance can only be evaluate the current and upcoming docs
     */
    public NumericDocValues select(
        final SortedNumericUnsignedLongValues values,
        final long missingValue,
        final BitSet parentDocs,
        final DocIdSetIterator childDocs,
        int maxDoc,
        int maxChildren
    ) throws IOException {
        if (parentDocs == null || childDocs == null) {
            return FieldData.replaceMissing(DocValues.emptyNumeric(), missingValue);
        }

        return new AbstractNumericDocValues() {

            int lastSeenParentDoc = -1;
            long lastEmittedValue = missingValue;

            @Override
            public boolean advanceExact(int parentDoc) throws IOException {
                assert parentDoc >= lastSeenParentDoc : "can only evaluate current and upcoming parent docs";
                if (parentDoc == lastSeenParentDoc) {
                    return true;
                } else if (parentDoc == 0) {
                    lastEmittedValue = missingValue;
                    return true;
                }
                final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                final int firstChildDoc;
                if (childDocs.docID() > prevParentDoc) {
                    firstChildDoc = childDocs.docID();
                } else {
                    firstChildDoc = childDocs.advance(prevParentDoc + 1);
                }

                lastSeenParentDoc = parentDoc;
                lastEmittedValue = pick(values, missingValue, childDocs, firstChildDoc, parentDoc, maxChildren);
                return true;
            }

            @Override
            public int docID() {
                return lastSeenParentDoc;
            }

            @Override
            public long longValue() {
                return lastEmittedValue;
            }
        };
    }

    protected long pick(
        SortedNumericUnsignedLongValues values,
        long missingValue,
        DocIdSetIterator docItr,
        int startDoc,
        int endDoc,
        int maxChildren
    ) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static MultiValueMode readMultiValueModeFrom(StreamInput in) throws IOException {
        return in.readEnum(MultiValueMode.class);
    }

    /**
     * Copied from {@link Long#divideUnsigned(long, long)} and {@link Long#remainderUnsigned(long, long)}
     */
    private static long divideUnsignedAndRoundUp(long dividend, long divisor) {
        assert divisor > 0;
        final long q = (dividend >>> 1) / divisor << 1;
        final long r = dividend - q * divisor;
        final long quotient = q + ((r | ~(r - divisor)) >>> (Long.SIZE - 1));
        final long rem = r - ((~(r - divisor) >> (Long.SIZE - 1)) & divisor);
        return quotient + Math.round((double) rem / divisor);
    }
}
