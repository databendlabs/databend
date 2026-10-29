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

use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_statistics::Histogram;
use databend_common_statistics::NdvEstimate;
use databend_common_storage::MetaHLL;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnStatistics as FuseColumnStatistics;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::decode_column_hll;

/// Materialize HLL bounds before the sketch is reduced to column statistics.
/// The coverage count must come from the same metadata as the selected HLL.
pub(crate) fn column_ndv_estimates(
    additional: Option<&AdditionalStatsMeta>,
    statistics: Option<&TableSnapshotStatistics>,
) -> Result<(Option<HashMap<ColumnId, NdvEstimate>>, u64)> {
    let estimate_hll = |hll: &databend_common_storage::MetaHLL| {
        let estimate = hll.count_estimate();
        NdvEstimate::new(estimate.expected, estimate.upper)
    };
    if let Some(meta) = additional
        && let Some(raw) = meta.hll.as_ref()
        && !raw.is_empty()
    {
        let estimates = decode_column_hll(raw)?.map(|hlls| {
            hlls.iter()
                .map(|(id, hll)| (*id, estimate_hll(hll)))
                .collect()
        });
        Ok((estimates, meta.row_count))
    } else if let Some(stats) = statistics {
        Ok((
            Some(
                stats
                    .hll
                    .iter()
                    .map(|(id, hll)| (*id, estimate_hll(hll)))
                    .collect(),
            ),
            stats.row_count,
        ))
    } else {
        Ok((None, 0))
    }
}

/// A column statistics provider for fuse table.
#[derive(Default)]
pub struct FuseTableColumnStatisticsProvider {
    stats_row_count: u64,
    row_count: u64,
    column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
    histograms: HashMap<ColumnId, Histogram>,
    top_n: BlockTopN,
    count_min_sketch: BlockCountMinSketch,
}

impl FuseTableColumnStatisticsProvider {
    pub fn new(
        column_stats: HashMap<ColumnId, FuseColumnStatistics>,
        histograms: HashMap<ColumnId, Histogram>,
        top_n: BlockTopN,
        count_min_sketch: BlockCountMinSketch,
        column_distinct_values: Option<HashMap<ColumnId, NdvEstimate>>,
        stats_row_count: u64,
        row_count: u64,
    ) -> Self {
        let distinct_map = column_distinct_values.as_ref();
        let column_stats = column_stats
            .into_iter()
            .map(|(column_id, stat)| {
                // The HLL producer supplies its statistical interval and its
                // own coverage. Without one, the summary holds the sum of
                // per-block sketch estimates. The union cannot exceed the sum
                // of block uppers, so it bounds NDV from above but gives no
                // point. The summary covers every current row.
                let (ndv, covered_rows) = match distinct_map.and_then(|map| map.get(&column_id)) {
                    Some(ndv) => (Some(*ndv), stats_row_count),
                    None => (
                        // A zero sum cannot bound non-NULL rows: it marks
                        // unknown blocks, and all-NULL columns are handled
                        // before NDV is read.
                        stat.distinct_of_values.filter(|sum| *sum > 0).map(|sum| {
                            NdvEstimate::upper_bound(MetaHLL::estimate_interval(sum as f64).upper)
                        }),
                        row_count,
                    ),
                };
                let stat = BasicColumnStatistics {
                    min: stat.min.to_datum(),
                    max: stat.max.to_datum(),
                    ndv,
                    null_count: stat.null_count,
                    in_memory_size: stat.in_memory_size,
                };
                (column_id, stat.get_useful_stat(row_count, covered_rows))
            })
            .collect();
        Self {
            column_stats,
            histograms,
            top_n,
            count_min_sketch,
            stats_row_count,
            row_count,
        }
    }
}

impl ColumnStatisticsProvider for FuseTableColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        self.column_stats.get(&column_id).and_then(|s| s.as_ref())
    }

    fn num_rows(&self) -> Option<u64> {
        Some(self.row_count)
    }

    fn stats_num_rows(&self) -> Option<u64> {
        Some(self.stats_row_count)
    }

    fn average_size(&self, column_id: ColumnId) -> Option<u64> {
        self.column_stats.get(&column_id).and_then(|v| {
            v.as_ref()
                .and_then(|s| s.in_memory_size.checked_div(self.row_count))
        })
    }

    fn histogram(&self, column_id: ColumnId) -> Option<Histogram> {
        self.histograms.get(&column_id).cloned()
    }

    fn top_n(&self, column_id: ColumnId) -> Option<ColumnTopN> {
        self.top_n.get(&column_id).cloned()
    }

    fn count_min_sketch(&self, column_id: ColumnId) -> Option<ColumnCountMinSketch> {
        self.count_min_sketch.get(&column_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn provider(ndv: Option<NdvEstimate>, stats_rows: u64) -> FuseTableColumnStatisticsProvider {
        provider_with(
            Some(12),
            ndv.map(|ndv| HashMap::from([(7, ndv)])),
            stats_rows,
        )
    }

    fn provider_with(
        summary_ndv: Option<u64>,
        hlls: Option<HashMap<ColumnId, NdvEstimate>>,
        stats_rows: u64,
    ) -> FuseTableColumnStatisticsProvider {
        FuseTableColumnStatisticsProvider::new(
            HashMap::from([(7, FuseColumnStatistics {
                min: Scalar::Number(NumberScalar::Int64(0)),
                max: Scalar::Number(NumberScalar::Int64(10_000)),
                distinct_of_values: summary_ndv,
                null_count: 20,
                in_memory_size: 24_000,
            })]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            hlls,
            stats_rows,
            3000,
        )
    }

    #[test]
    fn test_hll_numeric_upper_reaches_provider() {
        let provider = provider(Some(NdvEstimate::new(12.0, 15.0)), 3000);
        assert_eq!(
            provider.column_statistics(7).unwrap().ndv,
            Some(NdvEstimate::new(12.0, 15.0))
        );
    }

    #[test]
    fn test_actual_sketch_upper_is_not_replaced_by_population_size() {
        let mut hll = databend_common_storage::MetaHLL::new();
        for value in 0..100 {
            hll.add_object(&value);
        }
        let estimate = hll.count_estimate();
        let ndv = NdvEstimate::new(estimate.expected, estimate.upper);
        let provider = provider(Some(ndv), 3000);
        assert_eq!(provider.column_statistics(7).unwrap().ndv, Some(ndv));
        assert!(ndv.upper < 2980.0);
    }

    #[test]
    fn test_uncovered_rows_extend_source_upper() {
        let provider = provider(Some(NdvEstimate::new(12.0, 15.0)), 100);
        assert_eq!(
            provider.column_statistics(7).unwrap().ndv,
            Some(NdvEstimate::new(17.0, 2915.0))
        );
    }

    #[test]
    fn test_block_ndv_sum_bounds_ndv_without_hll() {
        // Summary NDV is the sum of per-block MetaHLL estimates. Their summed
        // upper bounds NDV, and no point estimate is derived from it.
        let upper = MetaHLL::estimate_interval(12.0).upper;
        assert!((12.0..2980.0).contains(&upper));
        for hlls in [None, Some(HashMap::new())] {
            assert_eq!(
                provider_with(Some(12), hlls, 0)
                    .column_statistics(7)
                    .unwrap()
                    .ndv,
                Some(NdvEstimate::upper_bound(upper))
            );
        }
    }

    #[test]
    fn test_block_ndv_sum_covers_all_rows() {
        // Another column's HLL coverage must not extend this column's bound:
        // the summary is maintained for every current row.
        let other = HashMap::from([(8, NdvEstimate::new(12.0, 15.0))]);
        assert_eq!(
            provider_with(Some(12), Some(other), 100)
                .column_statistics(7)
                .unwrap()
                .ndv,
            Some(NdvEstimate::upper_bound(
                MetaHLL::estimate_interval(12.0).upper
            ))
        );
    }

    #[test]
    fn test_missing_or_zero_block_ndv_sum_falls_back_to_rows() {
        for summary_ndv in [None, Some(0)] {
            assert_eq!(
                provider_with(summary_ndv, None, 0)
                    .column_statistics(7)
                    .unwrap()
                    .ndv,
                Some(NdvEstimate::upper_bound(2980.0))
            );
        }
    }

    #[test]
    fn test_block_ndv_sum_is_capped_by_rows() {
        assert_eq!(
            provider_with(Some(2900), None, 0)
                .column_statistics(7)
                .unwrap()
                .ndv,
            Some(NdvEstimate::upper_bound(2980.0))
        );
    }

    #[test]
    fn test_hll_takes_precedence_over_block_ndv_sum() {
        let ndv = NdvEstimate::new(100.0, 120.0);
        assert_eq!(
            provider_with(Some(12), Some(HashMap::from([(7, ndv)])), 3000)
                .column_statistics(7)
                .unwrap()
                .ndv,
            Some(ndv)
        );
    }

    #[test]
    fn test_hll_and_coverage_are_selected_together() -> Result<()> {
        let mut hll = databend_common_storage::MetaHLL::new();
        for value in 0..100 {
            hll.add_object(&value);
        }
        let expected = hll.count_estimate();
        let mut stats = TableSnapshotStatistics::empty_with_id(Default::default());
        stats.row_count = 100;
        stats.hll.insert(7, hll.clone());
        let mut additional = AdditionalStatsMeta {
            row_count: 999,
            ..Default::default()
        };
        let (ndvs, rows) = column_ndv_estimates(Some(&additional), Some(&stats))?;
        assert_eq!(rows, 100);
        assert_eq!(
            ndvs.unwrap()[&7],
            NdvEstimate::new(expected.expected, expected.upper)
        );

        additional.hll = Some(
            databend_storages_common_table_meta::meta::encode_column_hll(&HashMap::from([(
                7, hll,
            )]))?,
        );
        let (ndvs, rows) = column_ndv_estimates(Some(&additional), Some(&stats))?;
        assert_eq!(rows, 999);
        assert_eq!(
            ndvs.unwrap()[&7],
            NdvEstimate::new(expected.expected, expected.upper)
        );
        let (ndvs, rows) = column_ndv_estimates(None, None)?;
        assert!(ndvs.is_none());
        assert_eq!(rows, 0);
        Ok(())
    }

    #[test]
    fn test_no_coverage_does_not_invent_a_point() {
        let provider = provider(Some(NdvEstimate::new(12.0, 15.0)), 0);
        assert_eq!(
            provider.column_statistics(7).unwrap().ndv,
            Some(NdvEstimate::upper_bound(2980.0))
        );
    }
}
