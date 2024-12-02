/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Filter operator for star tree data structure.
 *
 *  @opensearch.experimental
 *  @opensearch.internal
 */
public class StarTreeFilter {
    private static final Logger logger = LogManager.getLogger(StarTreeFilter.class);

    /**
     *   First go over the star tree and try to match as many dimensions as possible
     *   For the remaining columns, use star-tree doc values to match them
     */
    public static FixedBitSet getStarTreeResult(StarTreeValues starTreeValues, Map<String, Object> predicateEvaluators) throws IOException {
        Map<String, Object> queryMap = predicateEvaluators != null ? predicateEvaluators : Collections.emptyMap();
        StarTreeResult starTreeResult = traverseStarTree(starTreeValues, queryMap);

        // Initialize FixedBitSet with size maxMatchedDoc + 1
        FixedBitSet bitSet = new FixedBitSet(starTreeResult.maxMatchedDoc + 1);
        SortedNumericStarTreeValuesIterator starTreeValuesIterator = new SortedNumericStarTreeValuesIterator(
            starTreeResult.matchedDocIds.build().iterator()
        );

        // No matches, return an empty FixedBitSet
        if (starTreeResult.maxMatchedDoc == -1) {
            return bitSet;
        }

        // Set bits in FixedBitSet for initially matched documents
        while (starTreeValuesIterator.nextEntry() != NO_MORE_DOCS) {
            bitSet.set(starTreeValuesIterator.entryId());
        }

        // Temporary FixedBitSet reused for filtering
        FixedBitSet tempBitSet = new FixedBitSet(starTreeResult.maxMatchedDoc + 1);

        iterateOverAllDimensionValues(starTreeValues);

        // Goes here when max nodes in leaf is not full.
        // Process remaining predicate columns to further filter the results
        for (String remainingPredicateColumn : starTreeResult.remainingPredicateColumns) {
            logger.debug("remainingPredicateColumn : {}, maxMatchedDoc : {} ", remainingPredicateColumn, starTreeResult.maxMatchedDoc);

            // TODO : Handle keyword, IP data types
            SortedNumericStarTreeValuesIterator ndv = (SortedNumericStarTreeValuesIterator) starTreeValues.getDimensionValuesIterator(remainingPredicateColumn);

            // TODO : Handle keyword, IP data types and range queries.
            long queryValue = (long) queryMap.get(remainingPredicateColumn); // Get the query value directly

            // Clear the temporary bit set before reuse
            tempBitSet.clear(0, starTreeResult.maxMatchedDoc + 1);

            if (bitSet.length() > 0) {
                // Iterate over the current set of matched document IDs
                for (int entryId = bitSet.nextSetBit(0); entryId != DocIdSetIterator.NO_MORE_DOCS; entryId = (entryId + 1 < bitSet.length())
                    ? bitSet.nextSetBit(entryId + 1)
                    : DocIdSetIterator.NO_MORE_DOCS) {
                    if (ndv.advance(entryId) != StarTreeValuesIterator.NO_MORE_ENTRIES) {
                        final int valuesCount = ndv.docValueCount();
                        for (int i = 0; i < valuesCount; i++) {
                            long value = ndv.nextValue();
                            // TODO : Instead use matchNextValue(Object queryValue) which returns true if queryValue matches next value in
                            // iterator.
                            // Compare the value with the query value
                            if (value == queryValue) {
                            // TODO : Handle keyword, IP data types while performing match.
                            // TODO : Handle range queries or any query type by passing "queryValue" to an interface
                            // TODO : Moving the logic of deciding a match away from here and based on underlying query
                            // TODO : builder and field datatype.
                            // TODO : Call matchNextValue(Object queryValue) here
                                tempBitSet.set(entryId);  // Set bit for the matching entryId
                                break;  // No need to check other values for this entryId
                            }
                        }
                    }
                }
            }

            // Perform intersection of the current matches with the temp results for this predicate
            bitSet.and(tempBitSet);
        }

        return bitSet;  // Return the final FixedBitSet with all matches
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate dimensions that are not matched.
     */
    private static StarTreeResult traverseStarTree(StarTreeValues starTreeValues, Map<String, Object> queryMap) throws IOException {
        DocIdSetBuilder docsWithField = new DocIdSetBuilder(starTreeValues.getStarTreeDocumentCount());
        DocIdSetBuilder.BulkAdder adder;
        Set<String> globalRemainingPredicateColumns = null;
        StarTreeNode starTree = starTreeValues.getRoot();
        List<String> dimensionNames = starTreeValues.getStarTreeField()
            .getDimensionsOrder()
            .stream()
            .map(Dimension::getField)
            .collect(Collectors.toList());
        boolean foundLeafNode = starTree.isLeaf();
        assert foundLeafNode == false; // root node is never leaf
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTree);
        int currentDimensionId = -1;
        Set<String> remainingPredicateColumns = new HashSet<>(queryMap.keySet());
        int matchedDocsCountInStarTree = 0;
        int maxDocNum = -1;
        StarTreeNode starTreeNode;
        List<Integer> docIds = new ArrayList<>();

        while ((starTreeNode = queue.poll()) != null) {
            int dimensionId = starTreeNode.getDimensionId();
            if (dimensionId > currentDimensionId) {
                String dimension = dimensionNames.get(dimensionId);
                remainingPredicateColumns.remove(dimension);
                if (foundLeafNode && globalRemainingPredicateColumns == null) {
                    globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
                }
                currentDimensionId = dimensionId;
            }

            if (remainingPredicateColumns.isEmpty()) {
                int docId = starTreeNode.getAggregatedDocId();
                docIds.add(docId);
                matchedDocsCountInStarTree++;
                maxDocNum = Math.max(docId, maxDocNum);
                continue;
            }

            if (starTreeNode.isLeaf()) {
                for (long i = starTreeNode.getStartDocId(); i < starTreeNode.getEndDocId(); i++) {
                    docIds.add((int) i);
                    matchedDocsCountInStarTree++;
                    // FIXME starTreeNode#getEndDocId-1 will always be the maximum so one check is sufficient.
                    maxDocNum = Math.max((int) i, maxDocNum);
                }
                continue;
            }

            // We have still not reached all the dimensions containing the remaining predicates.

            String childDimension = dimensionNames.get(dimensionId + 1);
            StarTreeNode starNode = null;
            if (globalRemainingPredicateColumns == null || !globalRemainingPredicateColumns.contains(childDimension)) {
                starNode = starTreeNode.getChildStarNode();
            }

            if (remainingPredicateColumns.contains(childDimension)) {
                // TODO : Handle keyword, IP data types and range queries. Conversion from ordinal for keyword/IP data types
                long queryValue = (long) queryMap.get(childDimension); // Get the query value directly from the map
                StarTreeNode matchingChild = starTreeNode.getChildForDimensionValue(queryValue);
                if (matchingChild != null) {
                    queue.add(matchingChild);
                    foundLeafNode |= matchingChild.isLeaf();
                }
            } else {
                if (starNode != null) {
                    queue.add(starNode);
                    foundLeafNode |= starNode.isLeaf();
                } else {
                    Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        if (childNode.getStarTreeNodeType() != StarTreeNodeType.STAR.getValue()) {
                            queue.add(childNode);
                            foundLeafNode |= childNode.isLeaf();
                        }
                    }
                }
            }
        }

        adder = docsWithField.grow(docIds.size());
        for (int id : docIds) {
            adder.add(id);
        }
        return new StarTreeResult(
            docsWithField,
            globalRemainingPredicateColumns != null ? globalRemainingPredicateColumns : Collections.emptySet(),
            matchedDocsCountInStarTree,
            maxDocNum
        );
    }

    // TODO : Remove after Draft PR
    private static void iterateOverAllDimensionValues(StarTreeValues starTreeValues) throws IOException {
        String[] dimensionNames = new String[]{"verb", "status", "port", "response_code"};
        for (String dimensionName : dimensionNames) {
            StarTreeValuesIterator starTreeValuesIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
            int docId = 0;
            List<Integer> docIds = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            while (starTreeValuesIterator.advance(docId) != NO_MORE_DOCS) {
                docIds.add(starTreeValuesIterator.entryId());
                if (starTreeValuesIterator.docValueCount() == 0) {
                    System.out.println("Zero doc value count for dimension " + dimensionName);
                    values.add(null);
                } else {
                    for (int i = 1; i <= starTreeValuesIterator.docValueCount(); i++) {
                        if (starTreeValuesIterator instanceof SortedNumericStarTreeValuesIterator) {
                            values.add(((SortedNumericStarTreeValuesIterator) starTreeValuesIterator).nextValue());
                        } else if (starTreeValuesIterator instanceof SortedSetStarTreeValuesIterator) {
                            values.add(((SortedSetStarTreeValuesIterator) starTreeValuesIterator).nextOrd());
                        }
                    }
                }
                docId++;
            }
            System.out.printf("Dimension Values for dimension %s has size %s and are %s and docIds are %s", dimensionName, values.size(), values, docIds);
        }
    }

    /**
     * Helper class to wrap the result from traversing the star tree.
     * */
    private static class StarTreeResult {
        public final DocIdSetBuilder matchedDocIds;
        public final Set<String> remainingPredicateColumns;
        public final int numOfMatchedDocs;
        public final int maxMatchedDoc;

        public StarTreeResult(
            DocIdSetBuilder matchedDocIds,
            Set<String> remainingPredicateColumns,
            int numOfMatchedDocs,
            int maxMatchedDoc
        ) {
            this.matchedDocIds = matchedDocIds;
            this.remainingPredicateColumns = remainingPredicateColumns;
            this.numOfMatchedDocs = numOfMatchedDocs;
            this.maxMatchedDoc = maxMatchedDoc;
        }
    }
}
