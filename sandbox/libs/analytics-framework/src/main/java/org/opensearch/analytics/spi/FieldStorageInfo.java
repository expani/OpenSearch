/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Per-column storage metadata describing where doc values, indices and stored fields live.
 * Flows through the plan tree: each OpenSearchRelNode computes this for its
 * output columns from its input's metadata.
 *
 * @opensearch.internal
 */
public class FieldStorageInfo {

    private final String fieldName;
    private final String mappingType;
    private final FieldType fieldType;
    private final List<String> docValueFormats;
    private final List<String> indexFormats;
    private final List<String> storedFieldFormats;
    private final boolean derived;
    private final List<String> dependsOnPhysicalCols;

    public FieldStorageInfo(
        String fieldName,
        String mappingType,
        FieldType fieldType,
        List<String> docValueFormats,
        List<String> indexFormats,
        List<String> storedFieldFormats,
        boolean derived
    ) {
        // Physical col is its own physical dependency; derived defaults to no deps until a
        // caller (e.g. Project / Aggregate FSI computation) supplies them via the 8-arg ctor.
        this(fieldName, mappingType, fieldType, docValueFormats, indexFormats, storedFieldFormats, derived, derived ? List.of() : List.of(fieldName));
    }

    public FieldStorageInfo(
        String fieldName,
        String mappingType,
        FieldType fieldType,
        List<String> docValueFormats,
        List<String> indexFormats,
        List<String> storedFieldFormats,
        boolean derived,
        List<String> dependsOnPhysicalCols
    ) {
        this.fieldName = fieldName;
        this.mappingType = mappingType;
        this.fieldType = fieldType;
        this.docValueFormats = docValueFormats;
        this.indexFormats = indexFormats;
        this.storedFieldFormats = storedFieldFormats;
        this.derived = derived;
        this.dependsOnPhysicalCols = dependsOnPhysicalCols;
    }

    /** Creates a derived column (agg result, expression) with no physical storage and no deps.
     *  FieldType inferred from SqlTypeName. Use {@link #derivedColumn(String, SqlTypeName, List)}
     *  when the caller can supply the underlying physical-column dependencies. */
    public static FieldStorageInfo derivedColumn(String fieldName, SqlTypeName sqlTypeName) {
        return derivedColumn(fieldName, sqlTypeName, List.of());
    }

    /** Creates a derived column with explicit physical-column dependencies — the set of
     *  TableScan-level fields whose values flow into this column's computation. */
    public static FieldStorageInfo derivedColumn(String fieldName, SqlTypeName sqlTypeName, List<String> dependsOnPhysicalCols) {
        return new FieldStorageInfo(
            fieldName,
            sqlTypeName.getName(),
            FieldType.fromSqlTypeName(sqlTypeName),
            List.of(),
            List.of(),
            List.of(),
            true,
            dependsOnPhysicalCols
        );
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    /** Data formats holding doc values for this field (e.g. ["parquet"], ["parquet", "lucene"]). */
    public List<String> getDocValueFormats() {
        return docValueFormats;
    }

    /** Data formats holding an index for this field (e.g. ["lucene"], ["lucene", "tantivy"]). */
    public List<String> getIndexFormats() {
        return indexFormats;
    }

    /** True for computed columns (agg results, expressions) with no physical storage. */
    public boolean isDerived() {
        return derived;
    }

    /**
     * Names of the TableScan-level (physical) columns whose values flow into this column's
     * computation. For physical columns this is a singleton {@code [fieldName]}; for derived
     * columns it is the union of underlying physical cols across the expression / agg-call
     * tree. {@code COUNT(*)} and pure-literal projects produce an empty list — no physical
     * dependency.
     *
     * <p>Used by the QTF rewriter to derive the fetch list off the topmost operator's FSI
     * without re-walking RexNodes from scratch.
     *
     * <p>TODO: today we use string field names because they stay stable across narrowed-scan
     * rewrites and (future) Join/Union plans where int ordinals across multiple TableScans
     * become ambiguous. For very large plans the per-FSI string list can become a memory
     * hotspot — consider switching to int ordinals (rooted to the originating TableScan's
     * rowType) when single-scan throughput dominates and stability across rewrites can be
     * traded off.
     */
    public List<String> getDependsOnPhysicalCols() {
        return dependsOnPhysicalCols;
    }

    /**
     * Resolves a field by index from a fieldStorageInfos list, validating bounds and field type.
     * Throws if the index is out of bounds or the field type is unrecognized.
     */
    public static FieldStorageInfo resolve(List<FieldStorageInfo> fieldStorageInfos, int fieldIndex) {
        if (fieldIndex >= fieldStorageInfos.size()) {
            throw new IllegalStateException(
                "Field index [" + fieldIndex + "] out of bounds for fieldStorageInfos of size [" + fieldStorageInfos.size() + "]"
            );
        }
        FieldStorageInfo info = fieldStorageInfos.get(fieldIndex);
        if (info.getFieldType() == null) {
            throw new IllegalStateException(
                "Unrecognized field type [" + info.getMappingType() + "] for field [" + info.getFieldName() + "]"
            );
        }
        return info;
    }
}
