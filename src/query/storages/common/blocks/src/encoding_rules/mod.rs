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
use databend_common_expression::TableSchema;
use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use parquet::schema::types::ColumnPath;

pub mod delta_binary_packed;
pub mod delta_ordering;
pub mod page_limit;

pub mod dictionary;

/// Statistics about the ordering and delta characteristics of a column's values.
///
/// Used by DELTA_BINARY_PACKED encoding heuristics to determine if a column has
/// suitable data patterns for delta encoding (e.g., sorted sequences with stable deltas).
///
/// These metrics help distinguish between:
/// - Suitable candidates: [1,2,3,4,5] → monotonic=1.0, CV=0.0
/// - Less suitable candidates: [3,1,5,2,4] → monotonic≈0.5, CV>1.0 (shuffled, less efficient)
#[derive(Debug, Clone, Copy)]
pub struct DeltaOrderingStats {
    /// Fraction of adjacent value pairs that maintain monotonic order (either all ↑ or all ↓).
    ///
    /// Calculated as: max(non_decreasing_pairs, non_increasing_pairs) / total_pairs
    ///
    /// - 1.0 = fully sorted (all increasing or all decreasing)
    /// - 0.5 = random order (half increasing, half decreasing)
    /// - Values ≥0.99 indicate the column is near-monotonic, suitable for delta encoding
    ///
    /// This metric helps reject random permutations of contiguous ranges,
    /// which would match NDV+span criteria but compress less efficiently with DBP.
    pub monotonic_ratio: f64,

    /// Coefficient of Variation (CV) of absolute deltas between adjacent values.
    ///
    /// The Coefficient of Variation is a normalized measure of dispersion that allows comparing
    /// variability across datasets with different scales. It's defined as the ratio of standard
    /// deviation to mean: `CV = σ / μ`.
    ///
    /// For our use case, we compute CV on the absolute differences between consecutive values:
    /// - `abs_deltas = [|v[1]-v[0]|, |v[2]-v[1]|, ..., |v[n]-v[n-1]|]`
    /// - `CV = stddev(abs_deltas) / mean(abs_deltas)`
    ///
    /// ## Interpretation:
    /// - **CV = 0.0**: Fully uniform deltas (e.g., [1,2,3,4] → deltas=[1,1,1], σ=0)
    /// - **CV ≤ 0.1**: Very stable, tolerates ~1% anomalies (e.g., 99×delta=1, 1×delta=2)
    /// - **CV ≈ 1.0**: Standard deviation equals mean, significant variation
    /// - **CV > 1.0**: High dispersion, frequent large jumps (e.g., [1,2,10,11,100])
    ///
    /// ## Why CV matters for delta encoding:
    /// Delta Binary Packed encoding stores deltas using bit-packing. The bit-width required
    /// is determined by the maximum delta. If deltas vary widely (high CV), more bits are
    /// needed to store small deltas at the same width as large deltas, reducing compression efficiency.
    ///
    /// Example:
    /// - More efficient: [1,2,3,4,5] → deltas=[1,1,1,1] → 1-bit packing
    /// - Less efficient: [1,2,10,11,100] → deltas=[1,8,1,89] → 7-bit packing (max=89)
    ///
    /// **Threshold**: Values ≤0.1 indicate deltas are stable enough for efficient bit-packing.
    pub abs_delta_cv: f64,
}

pub struct ColumnPathsCache {
    cache: Option<HashMap<ColumnId, ColumnPath>>,
}

impl ColumnPathsCache {
    pub fn new() -> Self {
        Self { cache: None }
    }

    pub fn get_or_build(&mut self, table_schema: &TableSchema) -> &HashMap<ColumnId, ColumnPath> {
        if self.cache.is_none() {
            self.cache = Some(
                table_schema_arrow_leaf_paths(table_schema)
                    .into_iter()
                    .map(|(id, path)| (id, ColumnPath::from(path)))
                    .collect(),
            );
        }
        self.cache.as_ref().unwrap()
    }
}

/// Provides per column NDV statistics.
pub trait NdvProvider {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64>;
}

impl NdvProvider for &StatisticsOfColumns {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.get(column_id).and_then(|item| item.distinct_of_values)
    }
}

pub trait EncodingStatsProvider: NdvProvider {
    fn column_stats(&self, column_id: &ColumnId) -> Option<&ColumnStatistics>;

    fn column_delta_stats(&self, _column_id: &ColumnId) -> Option<&DeltaOrderingStats> {
        None
    }
}

pub struct ColumnStatsView<'a>(pub &'a StatisticsOfColumns);

impl<'a> NdvProvider for ColumnStatsView<'a> {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.0
            .get(column_id)
            .and_then(|item| item.distinct_of_values)
    }
}

impl<'a> EncodingStatsProvider for ColumnStatsView<'a> {
    fn column_stats(&self, column_id: &ColumnId) -> Option<&ColumnStatistics> {
        self.0.get(column_id)
    }
}
