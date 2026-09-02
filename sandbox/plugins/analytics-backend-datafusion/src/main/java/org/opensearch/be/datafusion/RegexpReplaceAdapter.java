/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/** Adapts {@code REGEXP_REPLACE} for DataFusion: expand {@code \Q…\E}, brace {@code $N}, append "g" flag. */
class RegexpReplaceAdapter implements ScalarFunctionAdapter {

    // FIXME [RemoveBeforeMerge] diagnostic logger for the anchored-specialization experiment.
    private static final Logger LOG = LogManager.getLogger(RegexpReplaceAdapter.class);

    private static final String REGEX_METACHARS = ".\\+*?^$()[]{}|/";

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() < 3 || original.getOperands().size() > 4) {
            return original;
        }
        RexNode patternOperand = original.getOperands().get(1);
        RexNode replacementOperand = original.getOperands().get(2);

        String patternStr = null;
        if (patternOperand instanceof RexLiteral patternLiteral) {
            patternStr = patternLiteral.getValueAs(String.class);
        }

        String rewrittenPattern = null;
        if (patternStr != null && patternStr.contains("\\Q")) {
            String rewritten = unquoteJavaRegex(patternStr);
            if (!patternStr.equals(rewritten)) {
                rewrittenPattern = rewritten;
            }
        }

        String rewrittenReplacement = null;
        if (replacementOperand instanceof RexLiteral replacementLiteral) {
            String replacement = replacementLiteral.getValueAs(String.class);
            if (replacement != null && replacement.indexOf('$') >= 0) {
                String rewritten = braceBackreferences(replacement);
                if (!replacement.equals(rewritten)) {
                    rewrittenReplacement = rewritten;
                }
            }
        }

        // ClickBench-43 per-fix experiment (regexp anchored specialization):
        // PPL/Spark regexp_replace and ClickHouse REGEXP_REPLACE are GLOBAL (replace-all), so we
        // normally append the "g" flag to the 3-arg form. But when the pattern is an anchored
        // whole-string match (^...$, no top-level alternation) it matches at most ONCE, so global
        // and first-match are identical — we keep the cheaper 3-arg first-match form and avoid the
        // global-scan overhead (measured ~2.5x on the q28 Referer pattern). Same-answer: fires only
        // on provably single-match patterns; any other/unknown pattern keeps global semantics.
        String effectivePattern = rewrittenPattern != null ? rewrittenPattern : patternStr;
        boolean singleMatchPattern = isAnchoredWholeMatch(effectivePattern);
        boolean appendGlobalFlag = original.getOperator() == SqlLibraryOperators.REGEXP_REPLACE_3
            && original.getOperands().size() == 3
            && !singleMatchPattern;
        // FIXME [RemoveBeforeMerge] anchored-specialization diagnostic.
        LOG.debug(
            "[regexp-anchored] pattern={} singleMatchPattern={} appendGlobalFlag={}",
            effectivePattern,
            singleMatchPattern,
            appendGlobalFlag
        );

        if (rewrittenPattern == null && rewrittenReplacement == null && !appendGlobalFlag) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        // makeLiteral(String) sizes CHAR to the new value; reusing original type would right-pad.
        List<RexNode> newOperands = new ArrayList<>(original.getOperands().size() + (appendGlobalFlag ? 1 : 0));
        newOperands.add(original.getOperands().get(0));
        newOperands.add(rewrittenPattern != null ? rexBuilder.makeLiteral(rewrittenPattern) : patternOperand);
        newOperands.add(rewrittenReplacement != null ? rexBuilder.makeLiteral(rewrittenReplacement) : replacementOperand);
        for (int i = 3; i < original.getOperands().size(); i++) {
            newOperands.add(original.getOperands().get(i));
        }
        if (appendGlobalFlag) {
            newOperands.add(rexBuilder.makeLiteral("g", rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true));
            return rexBuilder.makeCall(original.getType(), SqlLibraryOperators.REGEXP_REPLACE_PG_4, newOperands);
        }
        return rexBuilder.makeCall(original.getType(), original.getOperator(), newOperands);
    }

    /**
     * True iff {@code pattern} is a literal anchored whole-string match: starts with {@code ^},
     * ends with an unescaped {@code $}, and contains no top-level alternation ({@code |}). Such a
     * pattern can match at most once, so global replace-all is identical to first-match and the
     * "g" flag (and its global-scan cost) can be dropped without changing results. Conservative:
     * returns false for a null/non-literal pattern, an escaped trailing {@code \$}, or any {@code |}.
     */
    static boolean isAnchoredWholeMatch(String pattern) {
        if (pattern == null || pattern.length() < 2) {
            return false;
        }
        if (pattern.charAt(0) != '^' || pattern.charAt(pattern.length() - 1) != '$') {
            return false;
        }
        // Trailing '$' must be a real end-anchor, not an escaped literal '\$'.
        if (pattern.charAt(pattern.length() - 2) == '\\') {
            return false;
        }
        // Any alternation could admit a branch that is not whole-string-anchored → not provably single-match.
        if (pattern.indexOf('|') >= 0) {
            return false;
        }
        return true;
    }

    /** Wrap bare {@code $N} backreferences in braces, preserving {@code $$} and {@code ${…}}. */
    static String braceBackreferences(String replacement) {
        StringBuilder out = new StringBuilder(replacement.length());
        int i = 0;
        while (i < replacement.length()) {
            char c = replacement.charAt(i);
            if (c == '$' && i + 1 < replacement.length()) {
                char next = replacement.charAt(i + 1);
                if (next == '$') {
                    out.append("$$");
                    i += 2;
                    continue;
                }
                if (next == '{') {
                    int closeIdx = replacement.indexOf('}', i + 2);
                    if (closeIdx == -1) {
                        out.append(replacement, i, replacement.length());
                        return out.toString();
                    }
                    out.append(replacement, i, closeIdx + 1);
                    i = closeIdx + 1;
                    continue;
                }
                if (Character.isDigit(next)) {
                    int j = i + 1;
                    while (j < replacement.length() && Character.isDigit(replacement.charAt(j))) {
                        j++;
                    }
                    out.append("${").append(replacement, i + 1, j).append("}");
                    i = j;
                    continue;
                }
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /** Expand {@code \Q…\E} blocks to per-char escapes. Unterminated {@code \Q} runs to end. */
    static String unquoteJavaRegex(String regex) {
        StringBuilder out = new StringBuilder(regex.length());
        int i = 0;
        while (i < regex.length()) {
            if (i + 1 < regex.length() && regex.charAt(i) == '\\' && regex.charAt(i + 1) == 'Q') {
                int contentStart = i + 2;
                int closeIdx = regex.indexOf("\\E", contentStart);
                int contentEnd = (closeIdx == -1) ? regex.length() : closeIdx;
                for (int j = contentStart; j < contentEnd; j++) {
                    char c = regex.charAt(j);
                    if (REGEX_METACHARS.indexOf(c) >= 0) {
                        out.append('\\');
                    }
                    out.append(c);
                }
                i = (closeIdx == -1) ? regex.length() : closeIdx + 2;
            } else {
                out.append(regex.charAt(i));
                i++;
            }
        }
        return out.toString();
    }
}
