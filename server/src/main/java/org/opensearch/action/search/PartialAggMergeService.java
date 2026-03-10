/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.action.search;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * POC: Merges partial aggregation results (native Arrow batches) into final rows.
 * Implemented by the DataFusion plugin, registered at startup.
 */
public interface PartialAggMergeService {

    /** Singleton holder — plugin sets this at boot. */
    class Holder {
        private static volatile PartialAggMergeService INSTANCE;
        public static void set(PartialAggMergeService svc) { INSTANCE = svc; }
        public static PartialAggMergeService get() { return INSTANCE; }
    }

    /**
     * @param nativeBatches list of VectorSchemaRoot objects (as Object to avoid Arrow dependency in server)
     * @param mergeSql SQL query to run over the "partial" table for final aggregation
     * @return future of rows, each row is column-name → value
     */
    CompletableFuture<List<Map<String, Object>>> merge(List<Object> nativeBatches, String mergeSql);
}
