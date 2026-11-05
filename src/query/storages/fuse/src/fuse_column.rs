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
use std::collections::HashSet;

use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_statistics::Histogram;
use databend_common_statistics::NdvEstimate;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnStatistics as FuseColumnStatistics;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::decode_column_hll;
use log::warn;

/// Select the per-column HLL NDV points and the row count they cover. The
/// coverage count must come from the same metadata as the selected HLL.
pub(crate) fn column_distinct_values(
    additional: Option<&AdditionalStatsMeta>,
    statistics: Option<&TableSnapshotStatistics>,
) -> Result<(Option<HashMap<ColumnId, u64>>, u64)> {
    if let Some(meta) = additional
        && let Some(raw) = meta.hll.as_ref()
        && !raw.is_empty()
    {
        let ndvs = decode_column_hll(raw)?.map(|hlls| {
            hlls.iter()
                .map(|(id, hll)| (*id, hll.count() as u64))
                .collect()
        });
        Ok((ndvs, meta.row_count))
    } else if let Some(stats) = statistics {
        Ok((Some(stats.column_distinct_values()), stats.row_count))
    } else {
        Ok((None, 0))
    }
}

/// Derive one column's provider statistics from the snapshot summary.
///
/// An HLL point covers the rows its sketch was built from and is extrapolated
/// to the rest; its upper is the structural cap: the covered rows, one more
/// per uncovered row, and finally the non-NULL rows and the discrete domain.
///
/// Without an HLL, the summary's sum of per-block sketch points is only an
/// upper: it counts a value once per block that holds it, so it exceeds the
/// NDV by up to the block count on a low-cardinality column. See
/// [`NdvEstimate`] for why such a source is not a point. Over a single block
/// the sum is that block's sketch, an HLL over the whole column, and serves
/// as both the point and the upper.
///
/// Statistics of an Array or Map element column describe elements, not rows:
/// fuse counts element NULLs, which may exceed the row count, and the summary
/// does not record the element count. They are verified against an unknown
/// row count instead, which neither caps nor extrapolates their NDV. Any other
/// violation has no known source in fuse metadata and drops the column.
fn column_statistics(
    stat: &FuseColumnStatistics,
    hll: Option<u64>,
    stats_row_count: u64,
    row_count: u64,
    block_count: u64,
    is_element: bool,
) -> std::result::Result<BasicColumnStatistics, String> {
    let min = stat.min.clone().to_datum();
    let max = stat.max.clone().to_datum();
    let single_block = block_count == 1;
    let (ndv, num_rows) = if is_element {
        let ndv = match (hll, stat.distinct_of_values) {
            (Some(point), _) if point > 0 => NdvEstimate::new(point as f64, f64::MAX),
            (None, Some(sum)) if sum > 0 && single_block => NdvEstimate::exact(sum as f64),
            (None, Some(sum)) if sum > 0 => NdvEstimate::upper_bound(sum as f64),
            _ => NdvEstimate::upper_bound(f64::MAX),
        };
        (ndv, None)
    } else {
        let covered =
            |point: u64, rows: u64| NdvEstimate::new((point as f64).min(rows as f64), rows as f64);
        // A zero sum marks unknown blocks; all-NULL columns are bounded below.
        let sum = stat.distinct_of_values.filter(|sum| *sum > 0);
        let ndv = match (hll, sum) {
            // A zero HLL point marks unknown legacy metadata, and a point over
            // no covered rows cannot be extrapolated.
            (Some(point), _) if point > 0 && stats_row_count > 0 => {
                covered(point, stats_row_count).extrapolate(stats_row_count, row_count)
            }
            (_, Some(sum)) if single_block => {
                NdvEstimate::exact((sum as f64).min(row_count as f64))
            }
            (_, Some(sum)) => NdvEstimate::upper_bound((sum as f64).min(row_count as f64)),
            (_, None) => NdvEstimate::upper_bound(row_count as f64),
        };
        (ndv, Some(row_count))
    };
    let upper = BasicColumnStatistics::ndv_upper_bound(
        min.as_ref(),
        max.as_ref(),
        stat.null_count,
        num_rows,
    );
    BasicColumnStatistics::new(
        min,
        max,
        stat.null_count,
        stat.in_memory_size,
        ndv.bound(upper),
        num_rows,
    )
}

/// Column ids of the leaves below an Array or Map, in the order fuse assigns
/// leaf ids when it generates column statistics.
fn element_column_ids(schema: &TableSchema) -> HashSet<ColumnId> {
    fn collect(
        data_type: &TableDataType,
        is_element: bool,
        next_column_id: &mut ColumnId,
        ids: &mut HashSet<ColumnId>,
    ) {
        match data_type.remove_nullable() {
            TableDataType::Tuple { fields_type, .. } => {
                for inner in &fields_type {
                    collect(inner, is_element, next_column_id, ids);
                }
            }
            TableDataType::Array(inner) | TableDataType::Map(inner) => {
                collect(&inner, true, next_column_id, ids);
            }
            TableDataType::AggregateState { state_type, .. } => {
                collect(&state_type, is_element, next_column_id, ids);
            }
            _ => {
                if is_element {
                    ids.insert(*next_column_id);
                }
                *next_column_id += 1;
            }
        }
    }

    let mut ids = HashSet::new();
    for field in schema.fields() {
        let mut next_column_id = field.column_id;
        collect(field.data_type(), false, &mut next_column_id, &mut ids);
    }
    ids
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
        column_distinct_values: Option<HashMap<ColumnId, u64>>,
        stats_row_count: u64,
        row_count: u64,
        block_count: u64,
        schema: &TableSchema,
    ) -> Self {
        let distinct_map = column_distinct_values.as_ref();
        let element_columns = element_column_ids(schema);
        let column_stats = column_stats
            .into_iter()
            .map(|(column_id, stat)| {
                let hll = distinct_map.and_then(|map| map.get(&column_id)).copied();
                let is_element = element_columns.contains(&column_id);
                let stat = column_statistics(
                    &stat,
                    hll,
                    stats_row_count,
                    row_count,
                    block_count,
                    is_element,
                )
                .inspect_err(|err| {
                    warn!("drop invalid fuse statistics of column {column_id}: {err}");
                })
                .ok();
                (column_id, stat)
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
                .and_then(|s| s.in_memory_size().checked_div(self.row_count))
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
    use databend_common_expression::TableField;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_statistics::Datum;
    use databend_common_storage::MetaHLL;

    use super::*;

    /// Several blocks, so the summary's block NDV sum is only an upper.
    const BLOCKS: u64 = 3;

    /// 3000 rows, 20 NULL in [0, 10000]: the structural cap is 2980.
    fn provider(hll_point: u64, stats_rows: u64) -> FuseTableColumnStatisticsProvider {
        provider_with(Some(12), Some(HashMap::from([(7, hll_point)])), stats_rows)
    }

    fn provider_with(
        summary_ndv: Option<u64>,
        hlls: Option<HashMap<ColumnId, u64>>,
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
            BLOCKS,
            &TableSchema::empty(),
        )
    }

    fn ndv(provider: FuseTableColumnStatisticsProvider) -> NdvEstimate {
        provider.column_statistics(7).unwrap().ndv()
    }

    #[test]
    fn test_hll_point_takes_the_structural_upper() {
        // The HLL point wins over the summary's block NDV sum (12); the upper
        // is the structural cap.
        assert_eq!(ndv(provider(100, 3000)), NdvEstimate::new(100.0, 2980.0));
        // An overestimating sketch is capped by the rows.
        assert_eq!(ndv(provider(5000, 3000)), NdvEstimate::exact(2980.0));
    }

    #[test]
    fn test_zero_hll_point_is_unknown() {
        // A zero point over non-NULL rows marks unknown legacy metadata; the
        // block NDV sum (12) still caps.
        assert_eq!(ndv(provider(0, 3000)), NdvEstimate::upper_bound(12.0));
        assert_eq!(
            ndv(provider_with(None, Some(HashMap::from([(7, 0)])), 3000)),
            NdvEstimate::upper_bound(2980.0)
        );
    }

    #[test]
    fn test_uncovered_rows_extrapolate_the_point() {
        assert_eq!(ndv(provider(12, 100)), NdvEstimate::new(17.0, 2980.0));
    }

    #[test]
    fn test_no_coverage_does_not_invent_a_point() {
        // The block NDV sum (12) still caps.
        assert_eq!(ndv(provider(12, 0)), NdvEstimate::upper_bound(12.0));
        assert_eq!(
            ndv(provider_with(None, Some(HashMap::from([(7, 12)])), 0)),
            NdvEstimate::upper_bound(2980.0)
        );
    }

    #[test]
    fn test_block_ndv_sum_is_an_upper_without_hll() {
        // Summary NDV is the sum of per-block sketch points: a cap on the
        // union, not a point. It covers every current row, so another
        // column's HLL coverage does not extrapolate it.
        for hlls in [None, Some(HashMap::new()), Some(HashMap::from([(8, 12)]))] {
            assert_eq!(
                ndv(provider_with(Some(12), hlls, 100)),
                NdvEstimate::upper_bound(12.0)
            );
        }
        assert_eq!(
            ndv(provider_with(Some(5000), None, 0)),
            NdvEstimate::upper_bound(2980.0)
        );
    }

    #[test]
    fn test_missing_or_zero_block_ndv_sum_falls_back_to_rows() {
        for summary_ndv in [None, Some(0)] {
            assert_eq!(
                ndv(provider_with(summary_ndv, None, 0)),
                NdvEstimate::upper_bound(2980.0)
            );
        }
    }

    fn stat(min: Scalar, max: Scalar, null_count: u64) -> FuseColumnStatistics {
        FuseColumnStatistics {
            min,
            max,
            distinct_of_values: Some(5),
            null_count,
            in_memory_size: 24_000,
        }
    }

    fn int(value: i64) -> Scalar {
        Scalar::Number(NumberScalar::Int64(value))
    }

    fn provider_of(
        column_stats: HashMap<ColumnId, FuseColumnStatistics>,
        schema: &TableSchema,
    ) -> FuseTableColumnStatisticsProvider {
        FuseTableColumnStatisticsProvider::new(
            column_stats,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            None,
            0,
            3000,
            BLOCKS,
            schema,
        )
    }

    #[test]
    fn test_invalid_statistics_drop_the_column() {
        let string = |value: &str| Scalar::String(value.to_string());
        let provider = provider_of(
            HashMap::from([
                (1, stat(int(10), int(0), 20)),
                (2, stat(string("b"), string("a"), 20)),
                (3, stat(int(0), string("a"), 20)),
                (4, stat(int(0), Scalar::Null, 20)),
                (5, stat(int(0), int(10), 3001)),
            ]),
            &TableSchema::empty(),
        );
        for column_id in 1..=5 {
            assert!(
                provider.column_statistics(column_id).is_none(),
                "column {column_id}"
            );
        }
    }

    #[test]
    fn test_element_statistics_are_not_bounded_by_rows() {
        let int64 = || TableDataType::Number(NumberDataType::Int64);
        let schema = TableSchema::new(vec![
            TableField::new("k", int64()),
            TableField::new(
                "arr",
                TableDataType::Array(Box::new(int64().wrap_nullable())),
            ),
            TableField::new(
                "m",
                TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![TableDataType::String, int64()],
                })),
            ),
            TableField::new("t", TableDataType::Tuple {
                fields_name: vec!["a".to_string(), "b".to_string()],
                fields_type: vec![int64(), TableDataType::Array(Box::new(int64()))],
            }),
        ]);
        assert_eq!(element_column_ids(&schema), HashSet::from([1, 2, 3, 5]));

        // 5000 element NULLs over 3000 rows: the row column is invalid, the
        // element column keeps its facts and only its domain bounds the NDV.
        // Its block NDV sum (5) is an upper, as for row columns.
        let provider = provider_of(
            HashMap::from([
                (0, stat(int(0), int(10), 5000)),
                (1, stat(int(0), int(10), 5000)),
            ]),
            &schema,
        );
        assert!(provider.column_statistics(0).is_none());
        let element = provider.column_statistics(1).unwrap();
        assert_eq!(element.min(), Some(&Datum::Int(0)));
        assert_eq!(element.max(), Some(&Datum::Int(10)));
        assert_eq!(element.null_count(), 5000);
        assert_eq!(element.ndv(), NdvEstimate::upper_bound(5.0));
    }

    #[test]
    fn test_hll_and_coverage_are_selected_together() -> Result<()> {
        let mut hll = MetaHLL::new();
        for value in 0..100 {
            hll.add_object(&value);
        }
        let point = hll.count() as u64;
        let mut stats = TableSnapshotStatistics::empty_with_id(Default::default());
        stats.row_count = 100;
        stats.hll.insert(7, hll.clone());
        let mut additional = AdditionalStatsMeta {
            row_count: 999,
            ..Default::default()
        };
        let (ndvs, rows) = column_distinct_values(Some(&additional), Some(&stats))?;
        assert_eq!(rows, 100);
        assert_eq!(ndvs.unwrap()[&7], point);

        additional.hll = Some(
            databend_storages_common_table_meta::meta::encode_column_hll(&HashMap::from([(
                7, hll,
            )]))?,
        );
        let (ndvs, rows) = column_distinct_values(Some(&additional), Some(&stats))?;
        assert_eq!(rows, 999);
        assert_eq!(ndvs.unwrap()[&7], point);
        let (ndvs, rows) = column_distinct_values(None, None)?;
        assert!(ndvs.is_none());
        assert_eq!(rows, 0);
        Ok(())
    }
}
