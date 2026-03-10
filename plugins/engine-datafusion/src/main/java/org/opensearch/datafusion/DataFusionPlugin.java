/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.action.search.PartialAggMergeService;
import org.opensearch.datafusion.action.DataFusionMergeService;
import org.opensearch.datafusion.action.DataFusionAction;
import org.opensearch.datafusion.action.NodesDataFusionInfoAction;
import org.opensearch.datafusion.action.PartialAggAction;
import org.opensearch.datafusion.action.TransportNodesDataFusionInfoAction;
import org.opensearch.datafusion.action.TransportPartialAggAction;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.cache.CacheSettings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.spi.vectorized.DataFormat;
import org.opensearch.plugins.spi.vectorized.DataSourceCodec;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.plugins.SearchEnginePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION;
import static org.opensearch.datafusion.core.DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION;

/**
 * Main plugin class for OpenSearch DataFusion integration.
 * Extends FlightStreamPlugin to inherit Flight transport registration (NetworkPlugin).
 * This ensures "FLIGHT" transport is registered when stream transport flag is enabled.
 */
public class DataFusionPlugin extends FlightStreamPlugin implements SearchEnginePlugin {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);
    private DataFusionService dataFusionService;
    private final boolean isDataFusionEnabled;

    public DataFusionPlugin(Settings settings) {
        super(settings);
        this.isDataFusionEnabled = true;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Map<DataFormat, DataSourceCodec> dataSourceCodecs
    ) {
        // Flight components are created via Plugin.createComponents (inherited from FlightStreamPlugin)
        // This method is called separately by the framework for SearchEnginePlugin
        if (!isDataFusionEnabled) {
            return Collections.emptyList();
        }

        String spill_dir = Arrays.stream(environment.dataFiles()).findFirst().get().getParent().resolve("tmp").toAbsolutePath().toString();
        dataFusionService = new DataFusionService(dataSourceCodecs, clusterService, spill_dir);

        PartialAggMergeService.Holder.set(new DataFusionMergeService());

        for (DataFormat format : this.getSupportedFormats()) {
            dataSourceCodecs.get(format);
        }

        return Collections.singletonList(dataFusionService);
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(DataFormat.CSV);
    }

    @Override
    public SearchExecEngine<DatafusionContext, DatafusionSearcher, DatafusionReaderManager, DatafusionQuery>
        createEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot, ShardPath shardPath) throws IOException {
        return new DatafusionEngine(dataFormat, formatCatalogSnapshot, dataFusionService, shardPath);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        List<RestHandler> handlers = new ArrayList<>(super.getRestHandlers(
            settings, restController, clusterSettings, indexScopedSettings,
            settingsFilter, indexNameExpressionResolver, nodesInCluster
        ));
        if (isDataFusionEnabled) {
            handlers.add(new DataFusionAction());
        }
        return handlers;
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>(super.getSettings());
        settingList.add(DATAFUSION_MEMORY_POOL_CONFIGURATION);
        settingList.add(DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);
        settingList.addAll(Stream.of(CacheSettings.CACHE_SETTINGS, CacheSettings.CACHE_ENABLED)
            .flatMap(x -> x.stream()).collect(Collectors.toList()));
        return settingList;
    }

    @Override
    public List<ActionHandler<?, ?>> getActions() {
        List<ActionHandler<?, ?>> actions = new ArrayList<>(super.getActions());
        if (isDataFusionEnabled) {
            actions.add(new ActionHandler<>(NodesDataFusionInfoAction.INSTANCE, TransportNodesDataFusionInfoAction.class));
            actions.add(new ActionHandler<>(PartialAggAction.INSTANCE, TransportPartialAggAction.class));
        }
        return actions;
    }
}
