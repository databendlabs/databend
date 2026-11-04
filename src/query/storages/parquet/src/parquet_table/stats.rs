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
use std::sync::Arc;

use databend_common_catalog::plan::FullParquetMeta;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ParquetTableColumnStatisticsProvider;
use databend_common_statistics::Datum;
use databend_common_statistics::NdvEstimate;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use log::warn;

/// Column statistics accumulated over the row groups that report them.
#[derive(Default)]
struct RowGroupAccumulator {
    min: Option<Datum>,
    max: Option<Datum>,
    null_count: u64,
    in_memory_size: u64,
    /// Rows of the row groups added so far.
    rows: u64,
    /// Sum of the distinct counts, or of the row-group bounds where a count
    /// is missing.
    ndv_sum: f64,
    /// Whether any added row group lacks a distinct count.
    has_uncounted: bool,
    /// Row groups added so far that hold non-NULL values.
    value_row_groups: usize,
}

impl RowGroupAccumulator {
    /// A row group's distinct count describes that row group only. A single
    /// counted row group is the column's point. Several row groups sum to an
    /// upper, not a point: like the fuse block NDV sum, the sum counts a value
    /// once per row group that holds it (see `NdvEstimate`). A missing
    /// or zero count over non-NULL rows contributes the row group's own row and
    /// domain bound to the upper. An all-NULL row group holds no distinct value
    /// whether or not it reports a count.
    fn add(&mut self, stat: &ColumnStatistics, num_rows: u64) {
        let min = stat.min.clone().to_datum();
        let max = stat.max.clone().to_datum();
        let upper = BasicColumnStatistics::ndv_upper_bound(
            min.as_ref(),
            max.as_ref(),
            stat.null_count,
            Some(num_rows),
        );
        self.ndv_sum += match stat.distinct_of_values {
            Some(ndv) if ndv > 0 => (ndv as f64).min(upper),
            _ => {
                self.has_uncounted |= upper > 0.0;
                upper
            }
        };
        self.min = Datum::min(self.min.take(), min);
        self.max = Datum::max(self.max.take(), max);
        self.null_count += stat.null_count;
        self.in_memory_size += stat.in_memory_size;
        self.rows += num_rows;
        if num_rows > stat.null_count {
            self.value_row_groups += 1;
        }
    }

    /// Rows of files without row-group statistics were not added; each adds
    /// at most one distinct value and leaves no point.
    fn finish(self, num_rows: u64) -> Result<BasicColumnStatistics, String> {
        let (min, max) = match (self.min, self.max) {
            (Some(min), Some(max)) => (Some(min), Some(max)),
            _ => (None, None),
        };
        let uncovered = num_rows.saturating_sub(self.rows);
        let ndv = if self.has_uncounted || uncovered > 0 || self.value_row_groups > 1 {
            NdvEstimate::upper_bound(self.ndv_sum + uncovered as f64)
        } else {
            NdvEstimate::exact(self.ndv_sum)
        };
        let upper = BasicColumnStatistics::ndv_upper_bound(
            min.as_ref(),
            max.as_ref(),
            self.null_count,
            Some(num_rows),
        );
        BasicColumnStatistics::new(
            min,
            max,
            self.null_count,
            self.in_memory_size,
            ndv.bound(upper),
            Some(num_rows),
        )
    }
}

// let's just read the first parquet meta and predict the whole stats
pub fn create_stats_provider(
    sample_metas: &[Arc<FullParquetMeta>],
    total_files: usize,
    num_columns: usize,
) -> ParquetTableColumnStatisticsProvider {
    let mut num_rows = 0;
    let mut accumulators: Vec<Option<RowGroupAccumulator>> =
        (0..num_columns).map(|_| None).collect();

    for meta in sample_metas {
        num_rows += meta
            .meta
            .row_groups()
            .iter()
            .map(|r| r.num_rows() as u64)
            .sum::<u64>();

        if total_files == sample_metas.len() {
            if let Some(stats) = &meta.row_group_level_stats {
                for (rg_stat, row_group) in stats.iter().zip(meta.meta.row_groups()) {
                    for (column_id, col_stat) in rg_stat {
                        accumulators[*column_id as usize]
                            .get_or_insert_with(Default::default)
                            .add(col_stat, row_group.num_rows() as u64);
                    }
                }
            }
        }
    }
    let ratio = if total_files > 0 {
        sample_metas.len() as f64 / total_files as f64
    } else {
        1.0
    };

    num_rows = (num_rows as f64 / ratio) as u64;
    let mut column_stats = HashMap::with_capacity(accumulators.len());
    for (column_id, accumulator) in accumulators.into_iter().enumerate() {
        let col_stat = accumulator.and_then(|accumulator| {
            accumulator
                .finish(num_rows)
                .inspect_err(|err| {
                    warn!("drop invalid parquet statistics of column {column_id}: {err}");
                })
                .ok()
        });
        column_stats.insert(column_id as u32, col_stat);
    }

    ParquetTableColumnStatisticsProvider::new(column_stats, num_rows)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn row_group(min: i64, max: i64, ndv: Option<u64>, nulls: u64) -> ColumnStatistics {
        ColumnStatistics {
            min: Scalar::Number(NumberScalar::Int64(min)),
            max: Scalar::Number(NumberScalar::Int64(max)),
            distinct_of_values: ndv,
            null_count: nulls,
            in_memory_size: 800,
        }
    }

    fn accumulate(row_groups: &[(ColumnStatistics, u64)]) -> BasicColumnStatistics {
        let mut accumulator = RowGroupAccumulator::default();
        for (stat, rows) in row_groups {
            accumulator.add(stat, *rows);
        }
        let rows = row_groups.iter().map(|(_, rows)| rows).sum();
        accumulator.finish(rows).unwrap()
    }

    #[test]
    fn test_single_row_group_count_is_exact() {
        let stats = accumulate(&[(row_group(0, 1000, Some(12), 20), 100)]);
        assert_eq!(stats.ndv(), NdvEstimate::exact(12.0));
        assert_eq!(stats.null_count(), 20);
        assert_eq!(stats.in_memory_size(), 800);
    }

    #[test]
    fn test_row_group_counts_sum_to_an_upper() {
        // Overlapping and disjoint row groups alike yield the sum as an upper;
        // the overlapping case has no point that the sum would not overstate.
        for second in [
            row_group(0, 1000, Some(5), 0),
            row_group(2000, 3000, Some(5), 0),
        ] {
            let stats = accumulate(&[(row_group(0, 1000, Some(12), 0), 100), (second, 100)]);
            assert_eq!(stats.ndv(), NdvEstimate::upper_bound(17.0));
        }
    }

    #[test]
    fn test_row_group_without_count_contributes_its_own_bound() {
        // Missing or zero counts over non-NULL rows fall back to that row
        // group's non-NULL rows and domain; the others keep their counts.
        for ndv in [None, Some(0)] {
            let stats = accumulate(&[
                (row_group(0, 1000, Some(12), 0), 100),
                (row_group(0, 1000, ndv, 10), 100),
                (row_group(0, 4, ndv, 0), 100),
            ]);
            assert_eq!(stats.ndv(), NdvEstimate::upper_bound(12.0 + 90.0 + 5.0));
        }
    }

    #[test]
    fn test_rows_without_row_group_statistics_leave_no_point() {
        // 100 of 300 rows come from a file without row-group statistics.
        let mut accumulator = RowGroupAccumulator::default();
        accumulator.add(&row_group(0, 1000, Some(12), 0), 100);
        accumulator.add(&row_group(0, 1000, Some(5), 0), 100);
        let stats = accumulator.finish(300).unwrap();
        assert_eq!(stats.ndv(), NdvEstimate::upper_bound(17.0 + 100.0));
    }

    #[test]
    fn test_all_null_row_group_adds_no_distinct_values() {
        let all_null = |ndv| ColumnStatistics {
            min: Scalar::Null,
            max: Scalar::Null,
            distinct_of_values: ndv,
            null_count: 100,
            in_memory_size: 0,
        };
        for ndv in [Some(0), None] {
            let stats = accumulate(&[(row_group(0, 1000, Some(12), 0), 100), (all_null(ndv), 100)]);
            assert_eq!(stats.ndv(), NdvEstimate::exact(12.0));
            assert_eq!(stats.min(), Some(&Datum::Int(0)));
            let stats = accumulate(&[(all_null(ndv), 100)]);
            assert_eq!(stats.ndv(), NdvEstimate::exact(0.0));
            assert_eq!(stats.min(), None);
        }
    }
}
