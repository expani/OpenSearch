/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import java.util.List;
import java.util.Map;

/**
 * Replacement for queryMap used across the star tree search code.
 */
public class StarTreePredicate {

    private final Map<String, List<QueryType>> dimensionNameToQueryTypes;

    StarTreePredicate(Map<String, List<QueryType>> dimensionNameToQueryTypes) {
        this.dimensionNameToQueryTypes = dimensionNameToQueryTypes;
    }

    static enum QueryType {
        TERM,
        RANGE;
    }

}
