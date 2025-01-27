/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Container for intermediate/consolidated dimension filters that will be applied for a query in star tree traversal.
 */
@ExperimentalApi
public class StarTreeFilter {

    private final Map<String, Set<DimensionFilter>> dimensionFilterMap;

    public StarTreeFilter(Map<String, Set<DimensionFilter>> dimensionFilterMap) {
        this.dimensionFilterMap = dimensionFilterMap;
    }

    public Set<DimensionFilter> getFiltersForDimension(String dimension) {
        return dimensionFilterMap.get(dimension);
    }

    public Set<String> getDimensions() {
        return dimensionFilterMap.keySet();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StarTreeFilter)) return false;
        StarTreeFilter that = (StarTreeFilter) o;
        return Objects.equals(dimensionFilterMap, that.dimensionFilterMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dimensionFilterMap);
    }
    // TODO : Implement Merging of 2 Star Tree Filters
    // This would also involve merging 2 different types of dimension filters.
    // It also brings in the challenge of sorting input values in user query for efficient merging.
    // Merging Range with Term and Range with Range and so on.
    // All these will be implemented post OS 2.19

}
