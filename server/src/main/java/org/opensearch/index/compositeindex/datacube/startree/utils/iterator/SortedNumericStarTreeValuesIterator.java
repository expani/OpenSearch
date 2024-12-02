/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils.iterator;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Wrapper iterator class for StarTree index to traverse through SortedNumericDocValues
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SortedNumericStarTreeValuesIterator extends StarTreeValuesIterator {

    public SortedNumericStarTreeValuesIterator(DocIdSetIterator docIdSetIterator) {
        super(docIdSetIterator);
    }

    @Override
    public int docValueCount() {
        return ((SortedNumericDocValues) docIdSetIterator).docValueCount();
    }

    public long nextValue() throws IOException {
        return ((SortedNumericDocValues) docIdSetIterator).nextValue();
    }

    public boolean advanceExact(int target) throws IOException {
        return ((SortedNumericDocValues) docIdSetIterator).advanceExact(target);
    }

    // TODO : We need a Iterator<Long> valuesInRange(long low, long high)
}
