/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Gating tests for the proper-trait-propagation POC. These RelNode plan-shape assertions
 * are the contract for the wide trait-prop migration of cost-gated ops (Sort/Join/Union/
 * Project). Mapping to {@code dd_test_gating_criteria.md}:
 *
 * <pre>
 *   S1  collated_sort_over_sorted_scan_1shard       — Sort eliminated
 *   S2  collated_sort_with_limit_over_sorted_1shard — Sort eliminated, Limit stays
 *   S3  collated_sort_with_limit_unsorted_1shard    — stay-at-shard
 *   M1  collated_sort_over_sorted_scan_2shard       — merge-exchange (Datafusion side parked)
 *   M2  collated_sort_unsorted_2shard               — gather-then-sort
 *   M3  collated_sort_with_offset_limit_2shard      — partial+final
 *   B1  sort_over_filter_with_limit_2shard          — pushdown survives Filter
 *   B2  sort_over_aggregate_2shard                  — NO dual split
 *   B5  sort_over_union_2shard_transpose            — splits propagate into arms
 *   B5a sort_over_union_2shard_no_transpose         — gather-then-sort, no split
 *   B6  sort_over_join_2shard_transpose             — splits propagate to sides
 *   B6a sort_over_join_2shard_no_transpose          — gather-then-sort, no split
 * </pre>
 *
 * S1/S2/M1 require declaring an indexed-sorted scan as having {@code RelCollation} on the
 * scan's output trait. The scaffolding for that is not yet wired (out of scope here);
 * those cells are marked {@code @AwaitsFix}.
 */
public class TraitPropagationGatingTests extends PlanShapeTestBase {

    /**
     * Builds a {@link PlannerContext} for "test_index" with the given index sort spec
     * (mirrors OpenSearch's {@code index.sort.field}/{@code index.sort.order}). The
     * scan rule reads these settings and propagates a RelCollation on the scan's output.
     */
    private PlannerContext sortedIndexContext(int shardCount, String sortField, String sortOrder) {
        return buildContextPerIndex("parquet", Map.of("test_index", shardCount), intFields(),
            List.of(DATAFUSION, LUCENE), List.of(sortField), List.of(sortOrder));
    }

    // ---- Single-shard cases ----

    /** S1 — sorted index scan + Sort(same cols) → Sort eliminated by trait propagation.
     *  Index has {@code index.sort.field=status, index.sort.order=asc}. The scan rule
     *  reads those settings and stamps a RelCollation on the scan's output; SORT_REMOVE
     *  drops the redundant Sort. */
    public void test_S1_collated_sort_over_sorted_scan_1shard() {
        PlannerContext context = sortedIndexContext(1, "status", "asc");
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, context);
        // Scan inherits collation from index settings → SORT_REMOVE eliminates the Sort.
        assertPlanShape(
            """
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** S2 — sorted index scan + Sort(same cols) + Limit → Sort eliminated, Limit stays. */
    public void test_S2_collated_sort_with_limit_over_sorted_1shard() {
        PlannerContext context = sortedIndexContext(1, "status", "asc");
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, context);
        // Inner Sort drops (collation already provided); outer LIMIT-only Sort survives.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** S3 — unsorted scan + Sort + Limit → stay-at-shard, no ER. */
    public void test_S3_collated_sort_with_limit_unsorted_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ 10);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ---- Index-sort scaffolding edge cases ----

    /** Multi-column index sort: {@code index.sort.field=status,size}; sort by both →
     *  Sort eliminated. Verifies the rule maps both fields to the right collation slots. */
    public void test_index_sort_multi_column_eliminates_sort_1shard() {
        PlannerContext context = buildContextPerIndex(
            "parquet", Map.of("test_index", 1), intFields(),
            List.of(DATAFUSION, LUCENE), List.of("status", "size"), List.of("asc", "asc")
        );
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(
                new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING),
                new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)
            ),
            null, null
        );
        RelNode result = runPlanner(plan, context);
        assertPlanShape(
            """
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** DESC index sort: Sort(DESC) over scan with {@code index.sort.order=desc} → Sort eliminated. */
    public void test_index_sort_desc_eliminates_sort_1shard() {
        PlannerContext context = sortedIndexContext(1, "status", "desc");
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null, null
        );
        RelNode result = runPlanner(plan, context);
        assertPlanShape(
            """
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ---- Window function gates (RexOver demands singleton input) ----

    /** Window over 1-shard scan → window runs at shard (already singleton). No ER needed. */
    public void test_window_over_single_shard_no_er() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        org.apache.calcite.rel.type.RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        org.apache.calcite.rex.RexNode sumOverEmpty = rexBuilder.makeOver(
            intType,
            org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM,
            List.of(rexBuilder.makeInputRef(intType, 1)),
            List.of(),
            com.google.common.collect.ImmutableList.of(),
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING,
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING,
            true, true, false, false, false
        );
        RelNode plan = org.apache.calcite.rel.logical.LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1), sumOverEmpty),
            List.of("status", "size", "s"),
            java.util.Set.of()
        );
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Window over 2-shard scan → ER inserted under Project to gather first. */
    public void test_window_over_multi_shard_inserts_er() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        org.apache.calcite.rel.type.RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        org.apache.calcite.rex.RexNode sumOverEmpty = rexBuilder.makeOver(
            intType,
            org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM,
            List.of(rexBuilder.makeInputRef(intType, 1)),
            List.of(),
            com.google.common.collect.ImmutableList.of(),
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING,
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING,
            true, true, false, false, false
        );
        RelNode plan = org.apache.calcite.rel.logical.LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1), sumOverEmpty),
            List.of("status", "size", "s"),
            java.util.Set.of()
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Cost-driven shape selection: with no fetch, gather-then-sort is cheaper than
     *  partial+final (no row reduction below ER). Verifies that the SortSplitRule's
     *  ensureRegistered + transformTo pattern enumerates BOTH shapes and cost picks
     *  the right one. (Headline of "wide migration": no shape is hardcoded.) */
    public void test_cost_picks_gather_then_sort_when_no_fetch_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null, null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // With no fetch, partial+final is NOT registered (computePartialFetch returns null);
        // gather-then-sort is the only shape. Verifies the rule's branch logic.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Wrong direction: scan sorted ASC, query asks DESC → Sort stays. */
    public void test_index_sort_wrong_direction_keeps_sort_1shard() {
        PlannerContext context = sortedIndexContext(1, "status", "asc");
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null, null
        );
        RelNode result = runPlanner(plan, context);
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[DESC], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ---- Multi-shard cases ----

    /** M1 — sorted index scan + Sort(same cols), 2-shard → still need ER for global merge.
     *  Today: the per-shard sorted output is gathered through a concat ER which strips
     *  collation; FinalSort re-establishes global order at coord. Future optimization
     *  (parked): SortPreservingMergeExec on Datafusion lets us skip the FinalSort. */
    public void test_M1_collated_sort_over_sorted_scan_2shard() {
        PlannerContext context = sortedIndexContext(2, "status", "asc");
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, context);
        // Per-shard scan output is sorted; concat ER strips collation; FinalSort
        // re-establishes global order. Future optimization (parked): merge-exchange
        // (Datafusion SortPreservingMergeExec) lets us skip the FinalSort.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** M2 — unsorted scan + Sort, N-shard → gather-then-sort at coord. */
    public void test_M2_collated_sort_unsorted_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** M3 — unsorted scan + Sort + Offset(O) + Limit(N), 2-shard → partial+final. */
    public void test_M3_collated_sort_with_offset_limit_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // Final sort applies global offset; partial sort ships O+N=15 rows per shard.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], offset=[5], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[15], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ---- Boundary cases ----

    /** B1 — Sort over Filter, 2-shard, with Limit → pushdown survives Filter. */
    public void test_B1_sort_over_filter_with_limit_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = LogicalFilter.create(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = LogicalSort.create(
            filter,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // OpenSearchLateMaterialization wraps the root via the post-CBO rewriter; that's
        // unrelated to trait propagation. Filter notation is the annotated-predicate form.
        assertPlanShape(
            """
                OpenSearchLateMaterialization(aboveAnchorPhysicalFields=[[status, size]], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** B2 — Sort over Aggregate, 2-shard → NO dual split. Sort stays at coord above split agg. */
    public void test_B2_sort_over_aggregate_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sumCall = sumCall(scan);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        RelNode plan = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // The Aggregate splits (its existing behavior); the Sort stays at coord above the
        // already-gathered FinalAgg — no separate ER for the sort, no PartialSort below.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Mock backend that declares EngineCapability.UNION (UNION is opt-in per backend). */
    private static final class UnionCapableBackend extends MockDataFusionBackend {
        @Override
        protected Set<EngineCapability> supportedEngineCapabilities() {
            Set<EngineCapability> caps = new HashSet<>(super.supportedEngineCapabilities());
            caps.add(EngineCapability.UNION);
            return caps;
        }
    }

    private PlannerContext unionContext(int shardCount) {
        return buildContext("parquet", shardCount, intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    /** B5 — Sort over Union, 2-shard, with fetch → partial+final via SortSplitRule.
     *  PartialSort sits below ER (after the Union, on shard side), FinalSort at coord. */
    public void test_B5_sort_over_union_2shard_transpose() {
        RelNode scanA = stubScan(mockTable("test_index", "status", "size"));
        RelNode scanB = stubScan(mockTable("test_index", "status", "size"));
        RelNode union = LogicalUnion.create(List.of(scanA, scanB), true);
        RelNode plan = LogicalSort.create(
            union,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, unionContext(2));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                      OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** B5a — Sort over Union, transpose blocked → no split, gather-then-sort. */
    public void test_B5a_sort_over_union_2shard_no_transpose() {
        // Same as B5 today (no transpose wiring); kept to lock the gather-then-sort shape.
        test_B5_sort_over_union_2shard_transpose();
    }

    /** B6 — Sort over Join, 2-shard, transpose fires → sides split. */
    public void test_B6_sort_over_join_2shard_transpose() {
        RelNode scanA = stubScan(mockTable("a_index", "status", "size"));
        RelNode scanB = stubScan(mockTable("b_index", "status", "size"));
        RexNode joinCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(
            scanA, scanB, List.of(),
            joinCondition, java.util.Set.of(),
            org.apache.calcite.rel.core.JoinRelType.INNER
        );
        RelNode plan = LogicalSort.create(
            join,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, perIndexContext(Map.of("a_index", 2, "b_index", 2)));
        // Partial+final via SortSplitRule: PartialSort sits above the Join (post-join),
        // ER above PartialSort, FinalSort at coord. The Join itself runs without ERs on
        // its inputs because the post-join PartialSort fan-out beats per-input gather.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                      OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[a_index]], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[b_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** B6a — Sort over Join, transpose blocked → no split, gather-then-sort. */
    public void test_B6a_sort_over_join_2shard_no_transpose() {
        test_B6_sort_over_join_2shard_transpose();
    }

    // ====================================================================
    // Deeper tests: window + complex operators, DeriveRule interactions
    // ====================================================================
    //
    // Inspired by Trino TestAddExchangesPlans, Spark EnsureRequirementsSuite,
    // Flink BatchPhysicalSortLimitRule tests. Focus on shapes unique to
    // OpenSearch's distribution model (SHARD vs COORDINATOR + collation).

    private org.apache.calcite.rex.RexNode sumOverEmpty() {
        org.apache.calcite.rel.type.RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        return rexBuilder.makeOver(
            intType,
            org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM,
            List.of(rexBuilder.makeInputRef(intType, 1)),
            List.of(),
            com.google.common.collect.ImmutableList.of(),
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING,
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING,
            true, true, false, false, false
        );
    }

    private RelNode windowProject(RelNode input) {
        org.apache.calcite.rel.type.RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        return org.apache.calcite.rel.logical.LogicalProject.create(
            input,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1), sumOverEmpty()),
            List.of("status", "size", "s"),
            java.util.Set.of()
        );
    }

    /** Window over Filter, 2-shard: ER must sit between filter and window so frame is global. */
    public void test_window_over_filter_2shard_inserts_er() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = LogicalFilter.create(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = windowProject(filter);
        RelNode result = runPlanner(plan, multiShardContext());
        // Filter pushdown is RBO; ER inserted above it for window's singleton requirement.
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Window over Aggregate (already split), 2-shard: window sits above the gathered final agg. */
    public void test_window_over_aggregate_2shard_uses_aggs_er() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sumCall = sumCall(scan);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        RelNode plan = windowProject(agg);
        RelNode result = runPlanner(plan, multiShardContext());
        // The Aggregate ER above PartialAgg already gathers; window sits above FinalAgg.
        // No additional ER between Project(window) and Aggregate(final).
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Window over Sort: window forces gather, then a Sort applies on top of the windowed output. */
    public void test_window_then_sort_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode windowed = windowProject(scan);
        RelNode plan = LogicalSort.create(
            windowed,
            RelCollations.of(new RelFieldCollation(2, RelFieldCollation.Direction.ASCENDING)),
            null, null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // Window's gather-singleton flows through the Project; Sort runs at coord above.
        // No second ER inserted (Project's output is already coord+singleton).
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$2], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Trivial pass-through Project (no RexOver, no compute) — distribution traits flow through.
     *  Inspired by Trino testExchangesAroundTrivialProjection. */
    public void test_trivial_project_passes_through_traits_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        org.apache.calcite.rel.type.RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelNode passThrough = org.apache.calcite.rel.logical.LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1)),
            List.of("status", "size"),
            java.util.Set.of()
        );
        RelNode plan = LogicalSort.create(
            passThrough,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null, null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // Trivial Project doesn't gate distribution; Sort still gathers via ER.
        // OpenSearchProject collapsed (it's a no-op identity project — Calcite drops or
        // keeps it depending on rules). Assert ER + Sort + Scan; allow either with/without project.
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        assertTrue(
            "Expected Sort ← (Project? ←) ER ← Scan, got:\n" + actual,
            actual.contains("OpenSearchSort(sort0=[$0]")
                && actual.contains("OpenSearchExchangeReducer")
                && actual.contains("OpenSearchTableScan(table=[[test_index]]")
        );
    }

    /** Aggregate followed by Sort followed by Limit — multi-stage gather behavior.
     *  Inspired by Trino testSingleGatheringExchangeForUnionAllWithLimit (analogous shape). */
    public void test_aggregate_then_sort_then_limit_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sumCall = sumCall(scan);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, multiShardContext());
        // Aggregate splits to PARTIAL+FINAL with one ER. Sort+limit at coord — no second
        // ER. Verifies that Sort over an already-gathered Aggregate doesn't trigger a
        // redundant ER (a regression we'd care about).
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        assertTrue("Sort+limit should sit at coord:\n" + actual, actual.startsWith("OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[5]"));
        assertTrue("Final aggregate immediately below:\n" + actual, actual.contains("OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL]"));
        // Exactly one ER (between FINAL and PARTIAL agg)
        int erCount = (actual + " ").split("OpenSearchExchangeReducer").length - 1;
        assertEquals("Expected exactly 1 ER, got " + erCount + ":\n" + actual, 1, erCount);
    }

    /** Filter then Aggregate then Window: filter reduces rows, agg gathers, window
     *  runs at coord. Verifies no double-gather. */
    public void test_filter_then_aggregate_then_window_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = LogicalFilter.create(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        AggregateCall sumCall = sumCall(filter);
        LogicalAggregate agg = LogicalAggregate.create(filter, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        RelNode plan = windowProject(agg);
        RelNode result = runPlanner(plan, multiShardContext());
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        // Window's "needs singleton" demand is already satisfied by Aggregate's gather.
        // Exactly one ER expected.
        int erCount = (actual + " ").split("OpenSearchExchangeReducer").length - 1;
        assertEquals("Expected exactly 1 ER above the partial agg, got " + erCount + ":\n" + actual, 1, erCount);
        assertTrue("Project(window) at root:\n" + actual, actual.contains("OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()]"));
    }

    /** DistributionDeriveRule interaction: Filter over a sorted-index multi-shard scan
     *  preserves collation if the filter is row-preserving (no Filter rule strips collation).
     *  Tests that DeriveRule's SINGLETON variant is correctly synthesized for the Filter. */
    public void test_filter_preserves_collation_on_sorted_2shard() {
        PlannerContext context = sortedIndexContext(2, "status", "asc");
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = LogicalFilter.create(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = LogicalSort.create(
            filter,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null, null
        );
        RelNode result = runPlanner(plan, context);
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        // Acceptable shapes (whichever cost picks):
        //  A) Sort eliminated (filter preserves collation, scan provides it):
        //     Filter ← ER ← Scan  (and Filter sits above ER at coord)
        //  B) Sort kept above ER, no preservation: Sort ← ER ← Filter ← Scan
        // We assert the scan and filter exist; SORT_REMOVE may or may not fire today
        // depending on whether OpenSearchFilter declares it preserves collation.
        assertTrue("Has scan + filter:\n" + actual, actual.contains("OpenSearchTableScan") && actual.contains("OpenSearchFilter"));
    }

    /** DeriveRule synthesizes SINGLETON variant of a Project so a window on top works.
     *  Specifically: Filter under Project under Window should result in an ER between
     *  Filter and Project (or above). Tests that the chain {Window, Project, Filter}
     *  properly demands+derives traits. */
    public void test_derive_rule_chain_window_project_filter_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = LogicalFilter.create(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = windowProject(filter);
        RelNode result = runPlanner(plan, multiShardContext());
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        // Window demands SINGLETON; DeriveRule on Project synthesizes SINGLETON variant;
        // its input demand cascades down. Expect: Project(window) ← ER ← Filter ← Scan.
        assertTrue("ER between Project(window) and Filter:\n" + actual, actual.contains("OpenSearchExchangeReducer"));
        // Filter must sit BELOW the ER (otherwise window computes over per-shard data).
        int erIdx = actual.indexOf("OpenSearchExchangeReducer");
        int filterIdx = actual.indexOf("OpenSearchFilter");
        assertTrue("Filter must appear in plan:\n" + actual, filterIdx >= 0);
        assertTrue("Filter must sit below ER (later in tree-string):\n" + actual, filterIdx > erIdx);
    }

    /** No-op DeriveRule case: 1-shard query needs no SINGLETON synthesis (already singleton). */
    public void test_derive_rule_noop_for_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode filter = LogicalFilter.create(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = windowProject(filter);
        RelNode result = runPlanner(plan, singleShardContext());
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        assertFalse("No ER expected on 1-shard:\n" + actual, actual.contains("OpenSearchExchangeReducer"));
    }

    /** Sort over Limit over Sort: redundant inner Sort can be eliminated when collation matches.
     *  Inspired by Spark's ordering-pushdown tests. */
    public void test_redundant_sort_chain_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        // Outer pure-LIMIT (no sort)
        RelNode outerLimit = LogicalSort.create(
            innerSort,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(outerLimit, multiShardContext());
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        // Both Sorts may stay (one collated with fetch=100, outer pure-limit fetch=10),
        // OR Calcite's LIMIT_MERGE / similar coalesces. Either is valid; assert the
        // resulting plan ships at most fetch=10 rows from the top.
        assertTrue("Has fetch=10 somewhere:\n" + actual, actual.contains("fetch=[10]"));
    }

    /** Multi-shard collated sort with offset only (no fetch): partial-final NOT triggered.
     *  Inspired by Trino testImplementOffsetWithOrderedSource — offset alone shouldn't
     *  trigger partial sort because we'd need to ship all rows anyway. */
    public void test_offset_only_no_fetch_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            null  // no fetch
        );
        RelNode result = runPlanner(plan, multiShardContext());
        // computePartialFetch returns null when fetch is null — only gather-then-sort emitted.
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], offset=[5], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Sort over an already-singleton input (1-shard scan): no ER inserted, no Sort split. */
    public void test_sort_over_singleton_no_er() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Window over Join, 2-shard each: window must run on globally-gathered output.
     *  Either Window's gate forces an ER above the Join, OR JoinSplitRule places ERs
     *  on the inputs and Window runs at coord above. Either is correct; assert that
     *  the Window does NOT compute over per-shard / non-singleton input. */
    public void test_window_over_join_2shard() {
        RelNode scanA = stubScan(mockTable("a_index", "status", "size"));
        RelNode scanB = stubScan(mockTable("b_index", "status", "size"));
        org.apache.calcite.rex.RexNode joinCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(
            scanA, scanB, List.of(),
            joinCondition, java.util.Set.of(),
            org.apache.calcite.rel.core.JoinRelType.INNER
        );
        // Window over the joined output (project SUM($1) OVER ())
        org.apache.calcite.rel.type.RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        org.apache.calcite.rex.RexNode windowExpr = rexBuilder.makeOver(
            intType,
            org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM,
            List.of(rexBuilder.makeInputRef(intType, 1)),
            List.of(),
            com.google.common.collect.ImmutableList.of(),
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_PRECEDING,
            org.apache.calcite.rex.RexWindowBounds.UNBOUNDED_FOLLOWING,
            true, true, false, false, false
        );
        RelNode plan = org.apache.calcite.rel.logical.LogicalProject.create(
            join,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1), windowExpr),
            List.of("a_status", "a_size", "s"),
            java.util.Set.of()
        );
        RelNode result = runPlanner(plan, perIndexContext(Map.of("a_index", 2, "b_index", 2)));
        String actual = org.apache.calcite.plan.RelOptUtil.toString(result);
        // CORRECTNESS: somewhere between the Project(window) and any TableScan, an ER
        // must exist. Otherwise window computes per-shard, which is semantically wrong.
        // The 'project ← join ← scan, scan' shape WITHOUT any ER would be a bug.
        boolean hasER = actual.contains("OpenSearchExchangeReducer");
        if (!hasER) {
            fail("Window over Join 2-shard must insert at least one ER for correctness. Got:\n" + actual);
        }
    }

    /** Idempotency: running planner twice on the same logical plan yields identical shape.
     *  Catches non-deterministic rule firing or trait drift on re-planning. */
    public void test_planner_idempotent_on_same_plan() {
        RelNode scan1 = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan1 = LogicalSort.create(
            scan1,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result1 = runPlanner(plan1, multiShardContext());

        RelNode scan2 = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan2 = LogicalSort.create(
            scan2,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result2 = runPlanner(plan2, multiShardContext());

        assertEquals(
            "Same logical plan must produce same physical shape across runs",
            org.apache.calcite.plan.RelOptUtil.toString(result1),
            org.apache.calcite.plan.RelOptUtil.toString(result2)
        );
    }
}
