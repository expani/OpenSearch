/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

/**
 * For a collated {@link OpenSearchSort}, emits the gather-then-sort alternative
 * ({@code Sort ← ER ← input}). When fetch is non-null, also emits the partial+final
 * alternative ({@code FinalSort(offset, fetch) ← ER ← PartialSort(fetch=offset+fetch)
 * ← input}) per the Flink BatchPhysicalSortLimitRule pattern. Volcano's cost model
 * picks the cheaper.
 *
 * <p>Pure LIMIT Sorts (empty collation) are skipped — partition-local fetch is correct.
 *
 * @opensearch.internal
 */
public class OpenSearchSortSplitRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchSortSplitRule(PlannerContext context) {
        super(operand(OpenSearchSort.class, any()), "OpenSearchSortSplitRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        if (sort.getCollation().getFieldCollations().isEmpty()) {
            return false; // pure LIMIT — skip
        }
        return !isSingleton(sort.getInput()) || !isSingleton(sort);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchSort sort = call.rel(0);
        // ER strips collation. Input demand at coord side: coord+singleton with EMPTY
        // collation; the Sort itself supplies the collation above the ER.
        RelTraitSet inputTraitsAtCoord = sort.getTraitSet()
            .replace(distTraitDef.coordSingleton())
            .replace(RelCollations.EMPTY);
        RelTraitSet sortTraitsAtCoord = sort.getTraitSet().replace(distTraitDef.coordSingleton());

        // Alternative A: gather-then-sort. Always emitted.
        RelNode gatheredInput = convert(sort.getInput(), inputTraitsAtCoord);
        RelNode gatherThenSort = sort.copy(sortTraitsAtCoord, gatheredInput, sort.getCollation(), sort.offset, sort.fetch);

        // Alternative B: partial+final split, only if fetch is a literal. Each shard
        // ships only `offset+fetch` rows; coord re-sorts and applies the global offset.
        RexNode partialFetch = computePartialFetch(sort);
        if (partialFetch != null) {
            RelNode partialSort = new OpenSearchSort(
                sort.getCluster(),
                sort.getInput().getTraitSet().replace(sort.getCollation()),
                sort.getInput(),
                sort.getCollation(),
                null,
                partialFetch,
                sort.getViableBackends(),
                false
            );
            RelNode gatheredPartial = convert(partialSort, inputTraitsAtCoord);
            RelNode finalSort = sort.copy(sortTraitsAtCoord, gatheredPartial, sort.getCollation(), sort.offset, sort.fetch);
            // Register the gather-then-sort variant; transform to partial+final.
            call.getPlanner().ensureRegistered(gatherThenSort, sort);
            call.transformTo(finalSort);
            return;
        }

        call.transformTo(gatherThenSort);
    }

    /**
     * Compute partial-side fetch = offset + fetch. Returns null when fetch is null
     * (no meaningful split). Constant-folds when both are literals; otherwise builds
     * a {@code +} expression so non-literal LIMITs (e.g. parameterized queries) still
     * get the partial+final optimization.
     */
    private static RexNode computePartialFetch(OpenSearchSort sort) {
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;
        if (fetch == null) {
            return null;
        }
        if (offset == null) {
            return fetch;
        }
        if (offset instanceof RexLiteral oLit && fetch instanceof RexLiteral fLit) {
            int o = ((Number) oLit.getValue4()).intValue();
            int f = ((Number) fLit.getValue4()).intValue();
            return sort.getCluster().getRexBuilder().makeLiteral(o + f, fLit.getType(), true);
        }
        return sort.getCluster()
            .getRexBuilder()
            .makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS, offset, fetch);
    }

    private static boolean isSingleton(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) {
                return dist.getType() == RelDistribution.Type.SINGLETON;
            }
        }
        return false;
    }
}
