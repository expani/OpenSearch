/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extending ValuesSourceAggregatorFactory for aggregation factories supported by star-tree implementation
 */
public abstract class MetricAggregatorFactory extends ValuesSourceAggregatorFactory {
    public MetricAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    public abstract MetricStat getMetricStat();

    @Override
    public boolean validateStarTreeSupport(CompositeDataCubeFieldType compositeIndexFieldInfo) {
        if (this.getSubFactories().getFactories().length == 0) {
            String field;
            Map<String, List<MetricStat>> supportedMetrics = compositeIndexFieldInfo.getMetrics()
                .stream()
                .collect(Collectors.toMap(Metric::getField, Metric::getMetrics));

            MetricStat metricStat = getMetricStat();
            field = config.fieldContext() == null ? null : config.fieldContext().field();

            return field != null && supportedMetrics.containsKey(field) && supportedMetrics.get(field).contains(metricStat);
        }
        return false;
    }
}
