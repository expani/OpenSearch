/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

public class StarTreeFilterRegistry {

    private final List<Query> queries = new ArrayList<>();

    private final List<QueryBuilder> queryBuilders = new ArrayList<>();

    // Can be used by @org.opensearch.search.aggregations.bucket.filter.FilterAggregator
    public void registerQuery(Query query) {
        queries.add(query);
    }

    public void registerQueryBuilder(QueryBuilder queryBuilder) {
        queryBuilders.add(queryBuilder);
    }

    public List<QueryBuilder> getQueryBuilders() {
        return new ArrayList<>(queryBuilders);
    }

    public List<Query> getQueries() {
        return new ArrayList<>(queries);
    }

}
