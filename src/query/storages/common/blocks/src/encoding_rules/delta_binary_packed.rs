// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use parquet::basic::Encoding;
use parquet::file::properties::WriterPropertiesBuilder;

use crate::encoding_rules::ColumnPathsCache;
use crate::encoding_rules::DeltaOrderingStats;

// NDV must be very close to row count (≥95%) to ensure high cardinality.
// This filters out columns with many duplicates where delta encoding usually brings limited gains.
const DELTA_HIGH_CARDINALITY_RATIO: f64 = 0.95;

// Span (max - min + 1) should be very close to NDV (≤1% tolerance).
// This ensures the range is contiguous with minimal gaps, making deltas small and stable.
// Example: [1,2,3,4,5] has span=5, NDV=5, ratio=1.0 ✓
//          [1,3,5,7,9] has span=9, NDV=5, ratio=1.8 ✗
const DELTA_RANGE_TOLERANCE: f64 = 1.01;

// At least 99% of adjacent pairs should maintain monotonic order (all ↑ or all ↓).
// This helps reject random permutations of contiguous ranges.
// Example: [1,2,3,4,5] → monotonic_ratio=1.0 ✓
//          [3,1,5,2,4] → monotonic_ratio≈0.5 ✗ (shuffled, inefficient for DBP)
const DELTA_MONOTONIC_RATIO: f64 = 0.99;

// Coefficient of variation for absolute deltas should be ≤0.1 (very stable).
// CV = stddev(abs_deltas) / mean(abs_deltas). Lower CV means more uniform deltas.
// This rejects sequences with occasional large jumps that inflate bit-width requirements.
// Example: [1,2,3,4,5,...]     → deltas=[1,1,1,...], CV=0.0 ✓
//          [1,2,3,10,11,12,...] → deltas=[1,1,7,1,1,...], CV≈1.2 ✗
const DELTA_ABS_DELTA_CV_LIMIT: f64 = 0.1;

pub fn apply_delta_binary_packed_heuristic(
    mut builder: WriterPropertiesBuilder,
    column_stats: &StatisticsOfColumns,
    delta_stats: &HashMap<ColumnId, DeltaOrderingStats>,
    table_schema: &TableSchema,
    num_rows: usize,
    column_paths_cache: &mut ColumnPathsCache,
) -> WriterPropertiesBuilder {
    for field in table_schema.leaf_fields() {
        // Restrict the DBP heuristic to INT32/UINT32 columns for now.
        // INT64 columns with high zero bits already compress well with PLAIN+Zstd, and other
        // widths need more validation before enabling DBP.
        if !matches!(
            field.data_type().remove_nullable(),
            TableDataType::Number(NumberDataType::Int32)
                | TableDataType::Number(NumberDataType::UInt32)
        ) {
            continue;
        }
        let column_id = field.column_id();
        let Some(stats) = column_stats.get(&column_id) else {
            continue;
        };
        let Some(ndv) = stats.distinct_of_values else {
            continue;
        };
        let Some(ordering_stats) = delta_stats.get(&column_id) else {
            continue;
        };
        if should_apply_delta_binary_packed(stats, ndv, num_rows, ordering_stats) {
            let column_paths = column_paths_cache.get_or_build(table_schema);
            if let Some(path) = column_paths.get(&column_id) {
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path.clone(), Encoding::DELTA_BINARY_PACKED);
            }
        }
    }
    builder
}

/// Decide whether to enable DELTA_BINARY_PACKED encoding for a 32-bit integer column.
///
/// ## Conservative Strategy
/// DBP has relatively high decode CPU cost, so we only enable it for near-sorted sequences
/// with stable deltas to ensure the compression benefit outweighs the decode overhead.
///
/// ## Checks Performed
/// 1. No NULLs (weakens contiguous-range signal)
/// 2. High cardinality: NDV/rows ≥ 0.95 (mostly unique values)
/// 3. Tight range: (max-min+1)/NDV ≤ 1.01 (contiguous, few gaps)
/// 4. Monotonic: ≥99% adjacent pairs move in same direction (rejects shuffled data)
/// 5. Stable deltas: CV(abs_deltas) ≤ 0.1 (rejects sequences with large jumps)
///
/// Target use case: auto-increment primary keys, sequential IDs, monotonic sequence numbers.
fn should_apply_delta_binary_packed(
    stats: &ColumnStatistics,
    ndv: u64,
    num_rows: usize,
    ordering_stats: &DeltaOrderingStats,
) -> bool {
    // Check 1: Reject if NULLs present (weakens contiguous-range assumption)
    if num_rows == 0 || ndv == 0 || stats.null_count > 0 {
        return false;
    }

    let Some(min) = scalar_to_i64(&stats.min) else {
        return false;
    };
    let Some(max) = scalar_to_i64(&stats.max) else {
        return false;
    };

    // Degenerate case: single value already compresses well
    if max <= min {
        return false;
    }

    // Check 2: High cardinality (NDV/rows ≥ 0.95)
    // Filters out columns with many duplicates where DBP provides no benefit
    let ndv_ratio = ndv as f64 / num_rows as f64;
    if ndv_ratio < DELTA_HIGH_CARDINALITY_RATIO {
        return false;
    }

    // Check 3: Tight range ((max-min+1)/NDV ≤ 1.01)
    // Ensures values form a nearly-contiguous sequence with minimal gaps
    let span = (max - min + 1) as f64;
    let contiguous_ratio = span / ndv as f64;
    if contiguous_ratio > DELTA_RANGE_TOLERANCE {
        return false;
    }

    // Check 4: Monotonicity (≥99% adjacent pairs in same direction)
    if ordering_stats.monotonic_ratio < DELTA_MONOTONIC_RATIO {
        return false;
    }

    // Check 5: Stable deltas (CV ≤ 0.1)
    if ordering_stats.abs_delta_cv > DELTA_ABS_DELTA_CV_LIMIT {
        return false;
    }

    true
}

fn scalar_to_i64(val: &Scalar) -> Option<i64> {
    // Only 32-bit integers reach the delta heuristic (see matches! check above),
    // so we deliberately reject other widths to avoid misinterpreting large values.
    match val {
        Scalar::Number(NumberScalar::Int32(v)) => Some(*v as i64),
        Scalar::Number(NumberScalar::UInt32(v)) => Some(*v as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::number::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::*;
    use crate::encoding_rules::delta_ordering::compute_delta_ordering_stats_i32;

    fn make_stats(min: i32, max: i32, null_count: u64) -> ColumnStatistics {
        ColumnStatistics {
            min: Scalar::Number(NumberScalar::Int32(min)),
            max: Scalar::Number(NumberScalar::Int32(max)),
            null_count,
            in_memory_size: 0,
            distinct_of_values: None,
        }
    }

    #[test]
    fn test_auto_increment_enables_dbp() {
        // Real data: [1, 2, 3, ..., 1000]
        let values: Vec<i32> = (1..=1000).collect();

        // Compute real ordering stats
        let ordering_stats = compute_delta_ordering_stats_i32(&values).unwrap();

        // Verify the computed stats match our expectations
        assert_eq!(
            ordering_stats.monotonic_ratio, 1.0,
            "Should be 100% monotonic"
        );
        assert_eq!(ordering_stats.abs_delta_cv, 0.0, "All deltas are 1, CV=0");

        // Compute column stats
        let stats = make_stats(1, 1000, 0);
        let ndv = 1000;
        let num_rows = 1000;

        // The decision should enable DBP
        assert!(
            should_apply_delta_binary_packed(&stats, ndv, num_rows, &ordering_stats),
            "Real auto-increment sequence should enable DBP"
        );
    }

    #[test]
    fn test_shuffled_sequence_rejected() {
        // Real data: manually shuffled [1..100] to ensure randomness
        // Pattern: reverse pairs, then reverse quartets, creating a chaotic sequence
        let mut values: Vec<i32> = Vec::new();
        for i in (1..=100).step_by(2) {
            if i < 100 {
                values.push(i + 1);
                values.push(i);
            } else {
                values.push(i);
            }
        }
        // values ≈ [2,1, 4,3, 6,5, ...] - heavily shuffled

        // Compute real ordering stats
        let ordering_stats = compute_delta_ordering_stats_i32(&values).unwrap();

        // Verify the computed stats show it's not monotonic
        assert!(
            ordering_stats.monotonic_ratio < 0.99,
            "Shuffled data should have low monotonicity, got {}",
            ordering_stats.monotonic_ratio
        );

        // Column stats still show contiguous range
        let stats = make_stats(1, 100, 0);
        let ndv = 100;
        let num_rows = 100;

        // Should be rejected due to low monotonicity
        assert!(
            !should_apply_delta_binary_packed(&stats, ndv, num_rows, &ordering_stats),
            "Shuffled sequence should be rejected despite matching NDV+span"
        );
    }

    #[test]
    fn test_large_jumps_rejected() {
        // Real data: [1,2,3,4,5, 100,101,102,103,104, 200,201,202,203,204, ...]
        // Has large jumps every 5 values
        let mut values = Vec::new();
        for i in 0..20 {
            for j in 0..5 {
                values.push(i * 100 + j + 1);
            }
        }
        // values = [1,2,3,4,5, 100,101,102,103,104, ...]

        // Compute real ordering stats
        let ordering_stats = compute_delta_ordering_stats_i32(&values).unwrap();

        // Verify monotonic but high CV
        assert_eq!(ordering_stats.monotonic_ratio, 1.0, "Still monotonic");
        assert!(
            ordering_stats.abs_delta_cv > 0.1,
            "Large jumps should cause high CV, got {}",
            ordering_stats.abs_delta_cv
        );

        // Column stats
        let stats = make_stats(1, 1904, 0); // min=1, max=1904
        let ndv = 100;
        let num_rows = 100;

        // Should be rejected due to high CV (or sparse range)
        assert!(
            !should_apply_delta_binary_packed(&stats, ndv, num_rows, &ordering_stats),
            "Sequence with large jumps should be rejected"
        );
    }
}
