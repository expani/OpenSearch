/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.datafusion.action;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.PartialAggMergeService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * POC: Implements PartialAggMergeService using DataFusion JNI.
 * Exports partial batches to FFI → Rust final agg → FFI result batch back → read rows.
 */
public class DataFusionMergeService implements PartialAggMergeService {

    private static final Logger logger = LogManager.getLogger(DataFusionMergeService.class);
    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE).newChildAllocator("final-agg", 0, Long.MAX_VALUE);

    @Override
    public CompletableFuture<List<Map<String, Object>>> merge(List<Object> nativeBatches, String mergeSql) {
        CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();

        try {
            int n = nativeBatches.size();
            long[] schemaPtrs = new long[n];
            long[] arrayPtrs = new long[n];
            // Keep references alive until Rust async task consumes them
            List<ArrowSchema> schemaRefs = new ArrayList<>(n);
            List<ArrowArray> arrayRefs = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                VectorSchemaRoot root = (VectorSchemaRoot) nativeBatches.get(i);
                logger.info("[POC] FinalAgg: input batch {}: rows={}, cols={}", i, root.getRowCount(),
                    root.getFieldVectors().stream().map(v -> v.getName() + ":" + v.getValueCount()).toList());

                // Export directly — batches are already deep copies from nextNativeBatch()
                var batchAllocator = root.getFieldVectors().get(0).getAllocator();
                ArrowSchema schema = ArrowSchema.allocateNew(batchAllocator);
                ArrowArray array = ArrowArray.allocateNew(batchAllocator);
                Data.exportVectorSchemaRoot(batchAllocator, root, null, array, schema);
                schemaPtrs[i] = schema.memoryAddress();
                arrayPtrs[i] = array.memoryAddress();
                schemaRefs.add(schema);
                arrayRefs.add(array);
            }

            logger.info("[POC] FinalAgg: exporting {} batches, mergeSql={}", n, mergeSql);

            NativeBridge.executeFinalAgg(schemaPtrs, arrayPtrs, mergeSql, new ActionListener<Long>() {
                // Prevent GC of FFI structs and source data until Rust consumes them
                private final List<ArrowSchema> keepSchemas = schemaRefs;
                private final List<ArrowArray> keepArrays = arrayRefs;

                @Override
                public void onResponse(Long pairPtr) {
                    try {
                        // Rust returned pointer to [schema_ptr: i64, array_ptr: i64]
                        long schemaPtr = UNSAFE.getLong(pairPtr);
                        long arrayPtr = UNSAFE.getLong(pairPtr + 8);

                        ArrowSchema ffiSchema = ArrowSchema.wrap(schemaPtr);
                        ArrowArray ffiArray = ArrowArray.wrap(arrayPtr);
                        VectorSchemaRoot result = Data.importVectorSchemaRoot(allocator, ffiArray, ffiSchema, null);

                        List<Map<String, Object>> rows = readBatch(result);
                        result.close();
                        future.complete(rows);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    private List<Map<String, Object>> readBatch(VectorSchemaRoot root) {
        List<Map<String, Object>> rows = new ArrayList<>();
        int numCols = root.getFieldVectors().size();
        for (int r = 0; r < root.getRowCount(); r++) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int c = 0; c < numCols; c++) {
                var vec = root.getFieldVectors().get(c);
                row.put(vec.getName(), vec.getObject(r));
            }
            rows.add(row);
        }
        logger.info("[POC] FinalAgg: read {} rows from result batch", rows.size());
        return rows;
    }
}
