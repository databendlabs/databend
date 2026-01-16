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

use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet::basic::Encoding;
use parquet::file::properties::WriterPropertiesBuilder;

use crate::encoding_rules::ColumnPathsCache;
use crate::encoding_rules::EncodingStatsProvider;

// NDV must be close to row count (~90%+). Empirical value based on experiments and operational experience.
const DELTA_HIGH_CARDINALITY_RATIO: f64 = 0.9;
// Span (max - min + 1) should be close to NDV. Empirical value based on experiments and operational experience.
const DELTA_RANGE_TOLERANCE: f64 = 1.05;

pub fn apply_delta_binary_packed_heuristic(
    mut builder: WriterPropertiesBuilder,
    metrics: &dyn EncodingStatsProvider,
    table_schema: &TableSchema,
    num_rows: usize,
    column_paths_cache: &mut ColumnPathsCache,
) -> WriterPropertiesBuilder {
    for field in table_schema.leaf_fields() {
        // Restrict the DBP heuristic to native INT32/UINT32 columns for now.
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
        let Some(stats) = metrics.column_stats(&column_id) else {
            continue;
        };
        let Some(ndv) = metrics.column_ndv(&column_id) else {
            continue;
        };
        if should_apply_delta_binary_packed(stats, ndv, num_rows) {
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

/// Evaluate whether Delta Binary Packed (DBP) is worth enabling for a 32-bit integer column.
///
/// The DBP heuristic rule is intentionally conservative:
/// - DBP is only considered when the block looks like a contiguous INT32/UINT32 range (no NULLs).
/// - NDV must be very close to the row count (`DELTA_HIGH_CARDINALITY_RATIO`).
/// - The `[min, max]` span should be close to NDV (`DELTA_RANGE_TOLERANCE`).
///   Experiments show that such blocks shrink dramatically after DBP + compression while decode CPU
///   remains affordable, yielding the best IO + CPU trade-off.
fn should_apply_delta_binary_packed(stats: &ColumnStatistics, ndv: u64, num_rows: usize) -> bool {
    // Nulls weaken the contiguous-range signal, so we avoid the heuristic when they exist.
    if num_rows == 0 || ndv == 0 || stats.null_count > 0 {
        return false;
    }
    let Some(min) = scalar_to_i64(&stats.min) else {
        return false;
    };
    let Some(max) = scalar_to_i64(&stats.max) else {
        return false;
    };
    // Degenerate spans (single value) already compress well without DBP.
    if max <= min {
        return false;
    }
    // Use ratio-based heuristics instead of absolute NDV threshold to decouple from block size.
    let ndv_ratio = ndv as f64 / num_rows as f64;
    if ndv_ratio < DELTA_HIGH_CARDINALITY_RATIO {
        return false;
    }
    let span = (max - min + 1) as f64;
    let contiguous_ratio = span / ndv as f64;
    contiguous_ratio <= DELTA_RANGE_TOLERANCE
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
