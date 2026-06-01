/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.metadata.IndexMetadata;

/**
 * OpenSearch-specific row-count metadata. Calcite's default {@link RelMdRowCount} returns
 * {@code 100.0} for a {@link org.apache.calcite.rel.core.TableScan} (a stub). This handler
 * provides a real estimate for {@link OpenSearchTableScan}: the sum of doc counts across
 * the backing concrete indices.
 *
 * <p>Cost-based planner decisions (gather-then-sort vs. partial+final, for instance) hinge
 * on this number being plausible. With the stub 100, partial+final cost is ~115 (Sort) +
 * ~110 (ER) + ~115 (Sort) ≈ 340; gather-then-sort is ~115 + ~110 ≈ 225. The latter wins for
 * 100 rows. With realistic counts (1M rows over 5 shards), partial+final's per-shard 200K
 * rows × log dominates; the answer flips. Tests that rely on cost-driven picks need this.
 *
 * <p><b>Plumbing TODOs (mainline-PR work):</b>
 * <ul>
 *   <li>{@link IndexResolution#totalDocCount()} returns -1 today — stub. Wire it to read
 *       {@code IndexService.indexStats().getDocs().getCount()} or equivalent. Care needed
 *       for: aliases/wildcards (sum across resolved concrete indices — see
 *       {@link IndexResolution}), data-stream backings, primary-only counting (avoid
 *       double-counting replicas), staleness (cluster-state-cached vs. live).</li>
 *   <li>{@code RelMdSelectivity}/{@code RelMdDistinctRowCount}: Filter selectivity and
 *       Aggregate cardinality — out of scope here, will need OpenSearch field-stats wiring
 *       (cardinality aggregation, term frequency stats). Calcite uses defaults of 0.25
 *       selectivity and assumes group-key NDV equals input row count, both very wrong.</li>
 *   <li>Histogram/quantile handlers ({@code RelMdSize}/{@code RelMdMinRowCount}/etc.) —
 *       same story. For the trait-prop POC we only need rough row counts; selectivity and
 *       cardinality are mainline work tracked in
 *       {@code dd_related_tasks.md}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class OpenSearchRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        new OpenSearchRelMdRowCount(),
        BuiltInMetadata.RowCount.Handler.class
    );

    private OpenSearchRelMdRowCount() {}

    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
        return BuiltInMetadata.RowCount.DEF;
    }

    /**
     * Row count for an {@link OpenSearchTableScan}. Returns Calcite's default (table's
     * statistic-row-count, typically 100) when the doc count isn't known.
     *
     * <p>Reflection-dispatched by {@link ReflectiveRelMetadataProvider}; method name and
     * parameter shape MUST match {@code BuiltInMetadata.RowCount.Handler.getRowCount}.
     */
    public Double getRowCount(OpenSearchTableScan scan, RelMetadataQuery mq) {
        // TODO: thread the IndexResolution / IndexMetadata list through OpenSearchTableScan
        // so this handler can ask for the live doc count without re-resolving. For now:
        // Calcite's default stub remains in effect (returns table.getRowCount() ≈ 100).
        // When wired:
        //   long docs = scan.getResolution().totalDocCount();
        //   return docs < 0 ? scan.getTable().getRowCount() : (double) docs;
        return scan.getTable().getRowCount();
    }

    /**
     * Helper for tests / future production wiring: estimate row count from a list of
     * backing {@link IndexMetadata}. Returns null when no index reports a usable doc count.
     */
    public static Double estimate(java.util.List<IndexMetadata> backing) {
        long total = 0L;
        boolean anyKnown = false;
        for (IndexMetadata md : backing) {
            // TODO: read md.getDocs() or equivalent; placeholder uses -1 sentinel via
            // IndexResolution. Once wired, swap to direct IndexMetadata access.
            // See IndexResolution.totalDocCount() for the resolver-aware version.
            long count = -1L;
            if (count >= 0) {
                total += count;
                anyKnown = true;
            }
        }
        return anyKnown ? (double) total : null;
    }
}
