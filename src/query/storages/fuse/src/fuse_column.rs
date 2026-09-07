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

use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchemaRef;
use databend_common_statistics::Histogram;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnStatistics as FuseColumnStatistics;
use databend_storages_common_table_meta::meta::ColumnTopN;

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
        schema: TableSchemaRef,
        stream_columns: Vec<StreamColumn>,
        histograms: HashMap<ColumnId, Histogram>,
        top_n: BlockTopN,
        count_min_sketch: BlockCountMinSketch,
        column_distinct_values: Option<HashMap<ColumnId, u64>>,
        stats_row_count: u64,
        row_count: u64,
    ) -> Self {
        let distinct_map = column_distinct_values.as_ref();
        let mut field_types = schema
            .leaf_fields()
            .into_iter()
            .map(|field| (field.column_id(), field.data_type().clone()))
            .collect::<HashMap<_, _>>();
        // `TableSchema::leaf_fields` deliberately skips engine-provided columns. Change
        // tracking persists statistics for stream columns, so include their canonical types
        // explicitly instead of dropping those statistics or reading them without a schema.
        field_types.extend(
            stream_columns
                .into_iter()
                .map(|column| (column.column_id(), column.table_data_type())),
        );
        let column_stats = column_stats
            .into_iter()
            .map(|(column_id, stat)| {
                let ndv = distinct_map
                    .and_then(|map| map.get(&column_id).cloned())
                    .or(stat.distinct_of_values)
                    .unwrap_or(row_count);
                let null_count = stat.null_count;
                let in_memory_size = stat.in_memory_size;
                let stat = field_types
                    .get(&column_id)
                    .and_then(|data_type| stat.try_view_with_table_type(data_type))
                    .map(|view| {
                        let (min, max) = view.datum_bounds();
                        BasicColumnStatistics {
                            min,
                            max,
                            ndv: Some(ndv),
                            null_count,
                            in_memory_size,
                        }
                    })
                    .and_then(|stat| stat.get_useful_stat(row_count, stats_row_count));
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_catalog::plan::StreamColumn;
    use databend_common_catalog::plan::StreamColumnType;
    use databend_common_catalog::table::ColumnStatisticsProvider;
    use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
    use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::decimal::DecimalScalar;
    use databend_common_expression::types::number::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::FuseTableColumnStatisticsProvider;

    fn stats(min: Scalar, max: Scalar) -> ColumnStatistics {
        ColumnStatistics::new(min, max, 0, 16, Some(2))
    }

    #[test]
    fn test_stream_column_statistics_use_canonical_type() {
        let user_field = TableField::new_from_column_id(
            "d",
            TableDataType::Decimal(DecimalDataType::Decimal64(DecimalSize::new(10, 2).unwrap())),
            0,
        );
        let schema = Arc::new(TableSchema::new(vec![user_field]));
        let stream_column = StreamColumn::new(
            ORIGIN_BLOCK_ROW_NUM_COL_NAME,
            StreamColumnType::OriginRowNum,
        );

        let mut column_stats = HashMap::new();
        column_stats.insert(
            0,
            stats(
                Scalar::Decimal(DecimalScalar::Decimal64(
                    100,
                    DecimalSize::new(10, 2).unwrap(),
                )),
                Scalar::Decimal(DecimalScalar::Decimal64(
                    200,
                    DecimalSize::new(10, 2).unwrap(),
                )),
            ),
        );
        column_stats.insert(
            ORIGIN_BLOCK_ROW_NUM_COLUMN_ID,
            stats(
                Scalar::Number(NumberScalar::UInt64(0)),
                Scalar::Number(NumberScalar::UInt64(9)),
            ),
        );
        // An unknown id must not fall back to interpreting persisted statistics without a type.
        column_stats.insert(
            42,
            stats(
                Scalar::Number(NumberScalar::UInt64(0)),
                Scalar::Number(NumberScalar::UInt64(9)),
            ),
        );

        let provider = FuseTableColumnStatisticsProvider::new(
            column_stats,
            schema,
            vec![stream_column],
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            None,
            10,
            10,
        );

        assert!(provider.column_statistics(0).is_some());
        assert!(
            provider
                .column_statistics(ORIGIN_BLOCK_ROW_NUM_COLUMN_ID)
                .is_some()
        );
        assert!(provider.column_statistics(42).is_none());
    }
}
