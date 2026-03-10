/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.arrow.flight.transport.FlightTransportChannel;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.DataFusionService;
import org.opensearch.datafusion.RecordBatchStream;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TaskTransportChannel;
import org.opensearch.transport.TransportChannel;

/**
 * DataNode handler: runs DataFusion Partial aggregation and streams native Arrow batches.
 */
public class TransportPartialAggAction extends TransportAction<PartialAggRequest, PartialAggAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPartialAggAction.class);
    private final IndicesService indicesService;
    private final DataFusionService dataFusionService;

    @Inject
    public TransportPartialAggAction(
        @Nullable StreamTransportService streamTransportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        DataFusionService dataFusionService
    ) {
        super(PartialAggAction.NAME, actionFilters,
            streamTransportService != null ? streamTransportService.getTaskManager() : null);
        this.indicesService = indicesService;
        this.dataFusionService = dataFusionService;

        if (streamTransportService != null) {
            logger.info("[POC] PartialAggAction registered with StreamTransportService");
            streamTransportService.registerRequestHandler(
                PartialAggAction.NAME,
                ThreadPool.Names.SEARCH,
                PartialAggRequest::new,
                this::handleStreamRequest
            );
        } else {
            logger.warn("[POC] StreamTransportService is null — stream transport feature flag not enabled?");
        }
    }

    @Override
    protected void doExecute(Task task, PartialAggRequest request, ActionListener<PartialAggAction.Response> listener) {
        listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
    }

    private void handleStreamRequest(PartialAggRequest request, TransportChannel channel, Task task) {
        logger.info("[POC] handleStreamRequest: index={}, shard={}", request.getIndexName(), request.getShardId());
        FlightTransportChannel flightChannel = unwrapChannel(channel);
        try {
            IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
            @SuppressWarnings("unchecked")
            EngineSearcherSupplier<DatafusionSearcher> supplier =
                (EngineSearcherSupplier<DatafusionSearcher>) shard.acquireSearcherSupplier();
            DatafusionSearcher searcher = supplier.acquireSearcher("partial-agg");

            long runtimePtr = dataFusionService.getRuntimePointer();
            DatafusionQuery query = new DatafusionQuery(
                request.getIndexName(),
                request.getSubstraitBytes(),
                null  // no search executors for partial agg
            );

            searcher.searchAsync(query, runtimePtr).whenComplete((streamPointer, error) -> {
                if (error != null) {
                    logger.error("[POC] searchAsync failed for shard={}", request.getShardId(), error);
                    try {
                        channel.sendResponse(new Exception("Partial agg failed", error));
                    } catch (Exception e) {
                        logger.error("Failed to send error response", e);
                    } finally {
                        searcher.close();
                    }
                    return;
                }

                BufferAllocator allocator = flightChannel.getAllocator();
                logger.info("[POC] searchAsync succeeded, streamPointer={}, starting batch streaming", streamPointer);
                RecordBatchStream stream = new RecordBatchStream(streamPointer, runtimePtr, allocator);
                streamBatches(stream, flightChannel, searcher);
            });
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception ex) {
                logger.error("Failed to send error", ex);
            }
        }
    }

    private void streamBatches(RecordBatchStream stream, FlightTransportChannel flightChannel, DatafusionSearcher searcher) {
        stream.loadNextBatch().whenComplete((hasMore, error) -> {
            if (error != null) {
                cleanup(stream, searcher, flightChannel, error);
                return;
            }
            if (!hasMore) {
                cleanup(stream, searcher, flightChannel, null);
                return;
            }
            try {
                flightChannel.sendNativeArrowBatch(stream.getVectorSchemaRoot());
                logger.info("[POC] sent native Arrow batch, rows={}", stream.getVectorSchemaRoot().getRowCount());
                // Recurse for next batch
                streamBatches(stream, flightChannel, searcher);
            } catch (Exception e) {
                cleanup(stream, searcher, flightChannel, e);
            }
        });
    }

    private void cleanup(RecordBatchStream stream, DatafusionSearcher searcher, FlightTransportChannel flightChannel, Throwable error) {
        try {
            stream.close();
        } catch (Exception e) {
            logger.warn("Error closing stream", e);
        }
        searcher.close();
        if (error != null) {
            logger.error("[POC] stream error, sending error response", error);
            try {
                flightChannel.sendResponse(new Exception("Stream error", error));
            } catch (Exception e) {
                logger.error("Failed to send error", e);
            }
        } else {
            logger.info("[POC] stream complete, calling completeStream()");
            flightChannel.completeStream();
        }
    }

    private FlightTransportChannel unwrapChannel(TransportChannel channel) {
        if (channel instanceof FlightTransportChannel ftc) return ftc;
        if (channel instanceof TaskTransportChannel ttc) return unwrapChannel(ttc.getChannel());
        throw new IllegalStateException("Expected FlightTransportChannel, got " + channel.getClass().getName());
    }
}
