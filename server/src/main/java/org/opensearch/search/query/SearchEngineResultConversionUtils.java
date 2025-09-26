/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ShardResultConvertor;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationFactory;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregator;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchEngineResultConversionUtils {

    private static final Logger LOGGER = LogManager.getLogger(SearchEngineResultConversionUtils.class);

    public static InternalAggregations convertDFResultGeneric(SearchContext searchContext) {
        // Fetch the result from DF
        Map<String, Object[]> dfResult = searchContext.getDFResults();

        // Create aggregators which will process the result from DataFusion
        try {

            List<Aggregator> aggregators = new ArrayList<>();

            if (searchContext.aggregations().factories().hasGlobalAggregator()) {
                aggregators.addAll(searchContext.aggregations().factories().createTopLevelGlobalAggregators(searchContext));
            }

            if (searchContext.aggregations().factories().hasNonGlobalAggregator()) {
                aggregators.addAll(searchContext.aggregations().factories().createTopLevelNonGlobalAggregators(searchContext));
            }

            List<ShardResultConvertor> shardResultConvertors = aggregators.stream().map(x -> {
                if (x instanceof ShardResultConvertor) {
                    return ((ShardResultConvertor) x);
                } else {
                    throw new UnsupportedOperationException("Aggregator doesn't support converting results from shard: " + x);
                }
            }).toList();

            return InternalAggregations.from(
                shardResultConvertors.stream().flatMap(x -> x.convert(dfResult).stream()).collect(Collectors.toList())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void printAggInfo(SearchContext searchContext) {
        AggregatorFactory[] aggregatorFactories = searchContext.aggregations().factories().getFactories();
        List<? extends Collector> collectors = searchContext.queryCollectorManagers().values().stream().map(x -> {
            try {
                return x.newCollector();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        collectors.forEach(x -> LOGGER.info("QryCollector {} and clazz {}", x, x.getClass().getName()));
        if (aggregatorFactories[0] instanceof CompositeAggregationFactory) {
            CompositeAggregationFactory compositeAggregationFactory = (CompositeAggregationFactory) aggregatorFactories[0];
            for (CompositeValuesSourceConfig valuesSourceConfig : compositeAggregationFactory.getSources()) {
                LOGGER.info(
                    "Value-Source type for field {} is {} mapped field-type {} doc-value format {}",
                    valuesSourceConfig.fieldType().name(),
                    valuesSourceConfig.valuesSource(),
                    valuesSourceConfig.fieldType().getClass(),
                    valuesSourceConfig.format()
                );
            }
            for (AggregatorFactory aggregatorFactory : compositeAggregationFactory.getSubFactories().getFactories()) {
                LOGGER.info("AggregatorFactory: " + aggregatorFactory);
            }
            CompositeAggregator compositeAggregator = (CompositeAggregator) collectors.getFirst();
            LOGGER.info(
                "CompositeAggregator {}",
                Arrays.toString(Arrays.stream(compositeAggregator.subAggregators()).map(x -> x.getClass().getName()).toArray(String[]::new))
            );
        }
        LOGGER.info("SearchContext Aggs: " + Arrays.toString(aggregatorFactories));
    }

}
