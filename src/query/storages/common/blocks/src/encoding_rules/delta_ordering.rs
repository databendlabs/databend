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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::number::NumberColumn;

use crate::encoding_rules::DeltaOrderingStats;

pub fn collect_delta_ordering_stats(
    schema: &TableSchema,
    block: &DataBlock,
) -> Result<HashMap<ColumnId, DeltaOrderingStats>> {
    let mut stats = HashMap::new();
    let mut leaves = Vec::new();
    traverse_values_dfs(block.columns(), schema.fields(), &mut leaves)?;
    for (column_id, value, _data_type) in leaves {
        let Value::Column(column) = value else {
            continue;
        };
        if let Some(ordering_stats) = column_delta_ordering_stats(&column) {
            stats.insert(column_id, ordering_stats);
        }
    }
    Ok(stats)
}

fn traverse_values_dfs(
    columns: &[BlockEntry],
    fields: &[TableField],
    leaves: &mut Vec<(ColumnId, Value<AnyType>, DataType)>,
) -> Result<()> {
    for (entry, field) in columns.iter().zip(fields) {
        let mut next_column_id = field.column_id;
        match entry {
            BlockEntry::Const(..) => {
                // Skip const entries: DBP is not suitable for it, and already skipped in `collect_delta_ordering_stats`
            }
            BlockEntry::Column(column) => {
                traverse_column_recursive(
                    column,
                    &column.data_type(),
                    &mut next_column_id,
                    leaves,
                )?;
            }
        }
    }
    Ok(())
}

fn traverse_column_recursive(
    column: &Column,
    data_type: &DataType,
    next_column_id: &mut ColumnId,
    leaves: &mut Vec<(ColumnId, Value<AnyType>, DataType)>,
) -> Result<()> {
    match data_type.remove_nullable() {
        DataType::Tuple(inner_types) => {
            let inner_columns = if data_type.is_nullable() {
                let nullable_column = column.as_nullable().unwrap();
                nullable_column.column.as_tuple().unwrap()
            } else {
                column.as_tuple().unwrap()
            };
            for (inner_column, inner_type) in inner_columns.iter().zip(inner_types.iter()) {
                traverse_column_recursive(inner_column, inner_type, next_column_id, leaves)?;
            }
        }
        DataType::Array(inner_type) => {
            let array_column = if data_type.is_nullable() {
                let nullable_column = column.as_nullable().unwrap();
                nullable_column.column.as_array().unwrap()
            } else {
                column.as_array().unwrap()
            };
            traverse_column_recursive(
                &array_column.underlying_column(),
                &inner_type,
                next_column_id,
                leaves,
            )?;
        }
        DataType::Map(inner_type) => match *inner_type {
            DataType::Tuple(inner_types) => {
                let map_column = if data_type.is_nullable() {
                    let nullable_column = column.as_nullable().unwrap();
                    nullable_column.column.as_map().unwrap()
                } else {
                    column.as_map().unwrap()
                };
                let kv_column = KvPair::<AnyType, AnyType>::try_downcast_column(
                    &map_column.underlying_column(),
                )?;
                traverse_column_recursive(
                    &kv_column.keys,
                    &inner_types[0],
                    next_column_id,
                    leaves,
                )?;
                traverse_column_recursive(
                    &kv_column.values,
                    &inner_types[1],
                    next_column_id,
                    leaves,
                )?;
            }
            // Map types are always encoded as Tuple(key, value)
            _ => unreachable!(),
        },
        // Only collect Int32/UInt32 columns for delta ordering stats
        DataType::Number(NumberDataType::Int32) | DataType::Number(NumberDataType::UInt32) => {
            leaves.push((
                *next_column_id,
                Value::Column(column.clone()),
                data_type.clone(),
            ));
            *next_column_id += 1;
        }
        _ => {
            // Skip other types - DBP encoding only applies to Int32/UInt32
            *next_column_id += 1;
        }
    }
    Ok(())
}

fn column_delta_ordering_stats(column: &Column) -> Option<DeltaOrderingStats> {
    match column {
        Column::Nullable(nullable) => {
            // Conservative strategy: we currently skip DBP encoding for columns with null values.
            // This could be optimized in the future by filtering out nulls before computing stats.
            if nullable.validity().null_count() != 0 {
                return None;
            }
            column_delta_ordering_stats(&nullable.column)
        }
        Column::Number(NumberColumn::Int32(values)) => {
            compute_delta_ordering_stats_i32(values.as_slice())
        }
        Column::Number(NumberColumn::UInt32(values)) => {
            compute_delta_ordering_stats_u32(values.as_slice())
        }
        _ => None,
    }
}

/// Computes ordering statistics to guide delta encoding decisions.
///
/// Returns `DeltaOrderingStats` with two metrics:
/// - `monotonic_ratio`: Fraction of adjacent pairs that maintain order (all ↑ or all ↓)
/// - `abs_delta_cv`: Coefficient of variation of absolute deltas (measures delta stability)
///
/// Returns `None` if the column has fewer than 2 values.
fn compute_delta_ordering_stats_impl<T, F>(values: &[T], to_i64: F) -> Option<DeltaOrderingStats>
where
    T: Copy,
    F: Fn(T) -> i64,
{
    if values.len() < 2 {
        return None;
    }

    // Track monotonicity (how many adjacent pairs maintain order)
    let mut prev = to_i64(values[0]);
    let mut non_decreasing = 0usize;
    let mut non_increasing = 0usize;

    // Welford's algorithm state for computing variance of absolute deltas
    let mut mean = 0.0;
    let mut m2 = 0.0;
    let mut count = 0usize;

    for &value in &values[1..] {
        let curr = to_i64(value);
        let delta = curr - prev;

        // Count monotonic pairs
        if delta >= 0 {
            non_decreasing += 1;
        }
        if delta <= 0 {
            non_increasing += 1;
        }

        // Update variance calculation with this delta
        let abs_delta = delta.abs() as f64;
        count += 1;
        let delta_mean = abs_delta - mean;
        mean += delta_mean / count as f64;
        m2 += delta_mean * (abs_delta - mean);

        prev = curr;
    }

    if count == 0 {
        return None;
    }

    // Compute coefficient of variation (CV = σ / μ)
    let variance = if count > 1 {
        m2 / (count as f64 - 1.0)
    } else {
        0.0
    };
    let abs_delta_cv = if mean == 0.0 {
        0.0
    } else {
        variance.sqrt() / mean
    };

    // Monotonic ratio: fraction of pairs in the dominant direction (ascending or descending)
    let monotonic_ratio = non_decreasing.max(non_increasing) as f64 / count as f64;

    Some(DeltaOrderingStats {
        monotonic_ratio,
        abs_delta_cv,
    })
}

/// Computes ordering statistics for Int32 columns.
pub(crate) fn compute_delta_ordering_stats_i32(values: &[i32]) -> Option<DeltaOrderingStats> {
    compute_delta_ordering_stats_impl(values, |v| v as i64)
}

/// Computes ordering statistics for UInt32 columns.
pub(crate) fn compute_delta_ordering_stats_u32(values: &[u32]) -> Option<DeltaOrderingStats> {
    compute_delta_ordering_stats_impl(values, |v| v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perfect_monotonic_sequence() {
        // [1, 2, 3, 4, 5] - perfect ascending
        let values = vec![1i32, 2, 3, 4, 5];
        let stats = compute_delta_ordering_stats_i32(&values).unwrap();

        assert_eq!(stats.monotonic_ratio, 1.0, "Should be 100% monotonic");
        assert_eq!(stats.abs_delta_cv, 0.0, "All deltas are 1, CV should be 0");
    }

    #[test]
    fn test_perfect_descending_sequence() {
        // [10, 9, 8, 7, 6] - perfect descending
        let values = vec![10i32, 9, 8, 7, 6];
        let stats = compute_delta_ordering_stats_i32(&values).unwrap();

        assert_eq!(
            stats.monotonic_ratio, 1.0,
            "Descending is also monotonic (100%)"
        );
        assert_eq!(stats.abs_delta_cv, 0.0, "All deltas are -1, CV should be 0");
    }

    #[test]
    fn test_shuffled_sequence() {
        // [3, 1, 5, 2, 4] - completely random
        let values = vec![3i32, 1, 5, 2, 4];
        let stats = compute_delta_ordering_stats_i32(&values).unwrap();

        // Deltas: [-2, 4, -3, 2]
        // non_decreasing: 2 (4, 2)
        // non_increasing: 2 (-2, -3)
        // max(2,2) / 4 = 0.5
        assert_eq!(
            stats.monotonic_ratio, 0.5,
            "Random order should have ~50% monotonic_ratio"
        );
        // abs_deltas: [2, 4, 3, 2]
        // mean = 11/4 = 2.75
        // CV should be moderate but might not be > 0.5 for this small sample
        assert!(
            stats.abs_delta_cv > 0.1,
            "Random deltas should have CV > 0.1, got {}",
            stats.abs_delta_cv
        );
    }

    #[test]
    fn test_sequence_with_occasional_jump() {
        // [1, 2, 3, 10, 11, 12] - has one large jump
        let values = vec![1i32, 2, 3, 10, 11, 12];
        let stats = compute_delta_ordering_stats_i32(&values).unwrap();

        // Deltas: [1, 1, 7, 1, 1]
        // All non-decreasing → monotonic_ratio = 1.0
        assert_eq!(stats.monotonic_ratio, 1.0, "All ascending");

        // mean = (1+1+7+1+1)/5 = 2.2
        // variance = ((1-2.2)^2*4 + (7-2.2)^2)/4 = (1.44*4 + 23.04)/4 = 29.8/4 = 7.45
        // stddev = sqrt(7.45) ≈ 2.73
        // CV = 2.73/2.2 ≈ 1.24
        assert!(
            stats.abs_delta_cv > 1.0,
            "Large jump should cause high CV (>1.0), got {}",
            stats.abs_delta_cv
        );
    }

    #[test]
    fn test_one_percent_anomaly() {
        // 99 deltas of 1, and 1 delta of 2
        let mut values = (1i32..=100).collect::<Vec<_>>();
        values[50] = 52; // Create one jump of 2 instead of 1

        let stats = compute_delta_ordering_stats_i32(&values).unwrap();

        assert_eq!(stats.monotonic_ratio, 1.0, "Still 100% ascending");

        // With 1% anomaly, CV should be < 0.1
        assert!(
            stats.abs_delta_cv < 0.15,
            "1% anomaly should have low CV, got {}",
            stats.abs_delta_cv
        );
    }

    #[test]
    fn test_uint32_values() {
        let values = vec![1u32, 2, 3, 4, 5];
        let stats = compute_delta_ordering_stats_u32(&values).unwrap();

        assert_eq!(stats.monotonic_ratio, 1.0);
        assert_eq!(stats.abs_delta_cv, 0.0);
    }

    #[test]
    fn test_single_value() {
        let values = vec![42i32];
        let stats = compute_delta_ordering_stats_i32(&values);

        assert!(stats.is_none(), "Single value should return None");
    }

    #[test]
    fn test_all_same_values() {
        // [5, 5, 5, 5] - all deltas are 0
        let values = vec![5i32, 5, 5, 5];
        let stats = compute_delta_ordering_stats_i32(&values).unwrap();

        // Deltas are all 0, which counts as both non-decreasing AND non-increasing
        assert_eq!(stats.monotonic_ratio, 1.0);
        // mean = 0, so CV = 0 by definition
        assert_eq!(stats.abs_delta_cv, 0.0);
    }
}
