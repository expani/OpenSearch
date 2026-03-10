# Distributed Partial Aggregate Reduction POC

## Overview

Coordinator fans out a query to DataNodes via stream transport. Each DataNode executes a partial aggregation using DataFusion, returning Arrow batches over Flight. The Coordinator collects all partial batches, loads them into a DataFusion MemTable, and runs a hardcoded merge SQL to produce the final result.

## Test Query

```
source=test_groupby | stats count() as cnt, sum(amount) as total by category | sort category
```

Setup: See [engine-datafusion README](README.md) for prerequisites (SQL plugin branch, maven local publish, gradle run command). The POC test uses a 2-shard index — create it with the steps below, then run the hardcoded query.

### Index Setup

```bash
# Delete if exists
curl -XDELETE localhost:9200/test_groupby

# Create with 2 shards
curl -X PUT "localhost:9200/test_groupby" -H 'Content-Type: application/json' -d'
{
  "settings": { "number_of_shards": 2, "number_of_replicas": 0, "index.optimized.enabled": true },
  "mappings": { "properties": {
    "category": { "type": "keyword" }, "amount": { "type": "integer" }
  }}
}'

# Bulk index 15 docs (categories A–E)
curl -X POST "localhost:9200/test_groupby/_bulk" -H 'Content-Type: application/json' -d'
{"index":{"_id":"11"}}
{"category":"A","amount":500}
{"index":{"_id":"12"}}
{"category":"A","amount":600}
{"index":{"_id":"13"}}
{"category":"A","amount":700}
{"index":{"_id":"14"}}
{"category":"A","amount":800}
{"index":{"_id":"15"}}
{"category":"B","amount":400}
{"index":{"_id":"16"}}
{"category":"B","amount":350}
{"index":{"_id":"17"}}
{"category":"B","amount":450}
{"index":{"_id":"18"}}
{"category":"B","amount":550}
{"index":{"_id":"19"}}
{"category":"C","amount":200}
{"index":{"_id":"20"}}
{"category":"C","amount":300}
{"index":{"_id":"21"}}
{"category":"D","amount":100}
{"index":{"_id":"22"}}
{"category":"D","amount":150}
{"index":{"_id":"23"}}
{"category":"D","amount":175}
{"index":{"_id":"24"}}
{"category":"E","amount":225}
{"index":{"_id":"25"}}
{"category":"E","amount":275}
'

curl -X POST "localhost:9200/test_groupby/_refresh"
```

### Run the Query

```bash
curl -X POST "localhost:9200/_plugins/_ppl" -H 'Content-Type: application/json' -d'
{"query": "source=test_groupby | stats count() as cnt, sum(amount) as total by category | sort category"}
'
```

## Hardcoded Merge SQL

The coordinator merge is hardcoded in `TransportSearchAction.executePartialAggFanout()` to this specific query shape:

```sql
SELECT category, SUM(cnt) as cnt, SUM(total) as total
FROM partial GROUP BY category ORDER BY category
```

The partial batches from each shard contain columns `category`, `cnt`, `total` (DataFusion preserves PPL aliases through `AggregateExec(Partial)`). The merge SQL sums the per-shard partial counts and sums.

## Expected Results

| category | cnt | total |
|----------|-----|-------|
| A        | 4   | 2600  |
| B        | 4   | 1750  |
| C        | 2   | 500   |
| D        | 3   | 425   |
| E        | 2   | 500   |

## Architecture

```
PPL Query
  │
  ▼
SQL Plugin → Substrait IR (queryPlanIR bytes)
  │
  ▼
TransportSearchAction.doExecute()
  ├── detects queryPlanIR != null
  ├── resolves shards → target nodes
  └── fans out PartialAggRequest per shard via stream transport
        │
        ▼
TransportPartialAggAction (DataNode)
  ├── registers Substrait plan with DataFusion
  ├── PartialAggregationOptimizer strips AggregateExec(Final), keeps Partial
  ├── executes partial aggregate → Arrow RecordBatch stream
  └── returns batches via Flight (StreamTransportResponse)
        │
        ▼
TransportSearchAction (Coordinator)
  ├── collects all partial batches (VectorSchemaRoot) from all shards
  ├── calls PartialAggMergeService.merge(batches, mergeSql)
  │     └── DataFusionMergeService exports batches via FFI → Rust
  │           └── executeFinalAgg: MemTable("partial") + SQL → result batch
  └── reads result rows, logs them
```

## Key Files

### New Files
| File | Purpose |
|------|---------|
| `server/.../PartialAggMergeService.java` | Interface for merge service (singleton holder pattern) |
| `server/.../PartialAggRequest.java` | Transport request: index, shardId, substraitBytes |
| `server/.../PartialAggResponse.java` | Transport response wrapping Arrow batches |
| `engine-datafusion/.../action/DataFusionMergeService.java` | Implements merge: FFI export → Rust final agg → FFI import result |
| `engine-datafusion/.../action/PartialAggAction.java` | Action definition for partial agg |
| `engine-datafusion/.../action/PartialAggRequest.java` | Plugin-side request handling |
| `engine-datafusion/.../action/TransportPartialAggAction.java` | DataNode handler: Substrait → DataFusion partial exec → stream response |

### Modified Files
| File | Change |
|------|--------|
| `TransportSearchAction.java` | Added `executePartialAggFanout()` — coordinator fanout + merge |
| `lib.rs` | Added `executeFinalAgg` JNI: FFI import → MemTable → SQL → FFI export |
| `partial_agg_optimizer.rs` | Strips `AggregateExec(Final)`, keeps `Partial` |
| `NativeBridge.java` | Added `executeFinalAgg(long[], long[], String, ActionListener)` |
| `DataFusionPlugin.java` | Registers `PartialAggAction`, `DataFusionMergeService` |
| `DatafusionEngine.java` | Exposes merge service registration |
| `FlightTransportResponse.java` | Support for streaming Arrow batches in transport |
| `StreamSearchTransportService.java` | Stream transport wiring |

## Issues Resolved

1. **SIGSEGV in FFI import** — `std::ptr::replace(..., FFI_ArrowArray::empty())` to take ownership
2. **Nested Tokio runtime** — `io_runtime.spawn` instead of `block_on`
3. **CrossRtStream nested runtime** — removed, use `df.collect().await` + `concat_batches`
4. **Schema mismatch** — merge SQL column names must match partial batch schema
5. **All-null data** — deep copy via `VectorUnloader`/`VectorLoader` in `nextNativeBatch()`
6. **Cross-allocator error** — export directly from batch allocator, no redundant copy
7. **Buffer alignment panic** — `ArrayData::align_buffers()` after FFI import

## Validated Run (2026-03-10)

```
PPL: source=test_groupby | stats count() as cnt, sum(amount) as total by category | sort category

Calcite Logical Plan:
  LogicalSystemLimit(fetch=10000)
    LogicalSort(sort=category ASC)
      LogicalProject(cnt, total, category)
        LogicalAggregate(group={category}, cnt=COUNT(), total=SUM(amount))
          LogicalProject(category, amount)
            CalciteLogicalIndexScan(table=test_groupby)

Substrait: AggregateRel with count: + sum:i32, phase=INITIAL_TO_RESULT, ReadRel(named_table=test_groupby)
Output names: [cnt, total, category]

Physical plan (per shard, after PartialAggregationOptimizer):
  ProjectionExec → SortExec(TopK) → ProjectionExec → AggregateExec(mode=Partial) → DataSourceExec(parquet)

Partial batch schema: {cnt: Int64, total: Int64, category: Utf8View}
Shard 0: 4 rows | Shard 1: 4 rows → 2 batches, 8 rows total

Merge: SELECT category, SUM(cnt) as cnt, SUM(total) as total FROM partial GROUP BY category ORDER BY category
Result: 5 rows — A:4/2600, B:4/1750, C:2/500, D:3/425, E:2/500
```

Known warning: DataNode `RecordBatchStream.close()` leaks 262 bytes per stream (see Limitations).

## Limitations

- Merge SQL is hardcoded to the test query shape (single GROUP BY + SUM/COUNT)
- No AVG support (requires SUM/COUNT decomposition)
- Result rows are logged but not wired into SearchResponse aggregation buckets
- Memory leak on DataNode: `RecordBatchStream.close()` warns about 198–262 leaked bytes
