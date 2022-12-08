// Copyright 2022 Datafuse Labs.
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

use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::statistics::BinaryStatistics;
use common_arrow::parquet::statistics::BooleanStatistics;
use common_arrow::parquet::statistics::PrimitiveStatistics;
use common_arrow::parquet::statistics::Statistics;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Scalar;
use common_storages_index::RangeFilter;
use common_storages_table_meta::meta::ColumnStatistics;
use common_storages_table_meta::meta::StatisticsOfColumns;

use crate::hive_parquet_block_reader::HiveParquetBlockReader;
use crate::hive_table::HIVE_DEFAULT_PARTITION;

#[derive(Clone)]
pub struct HiveBlockFilter {
    range_filter: Option<RangeFilter>,
    projections: Vec<DataField>,
    data_schema: Arc<DataSchema>,
}

impl HiveBlockFilter {
    pub fn create(
        range_filter: Option<RangeFilter>,
        projections: Vec<DataField>,
        data_schema: Arc<DataSchema>,
    ) -> Self {
        Self {
            range_filter,
            projections,
            data_schema,
        }
    }

    // true: rowgroup if filtered by predict
    pub fn filter(
        &self,
        row_group: &RowGroupMetaData,
        part_columns: HashMap<String, String>,
    ) -> bool {
        if let Some(filter) = &self.range_filter {
            let mut statistics = StatisticsOfColumns::new();
            for col in self.projections.iter() {
                let column_meta =
                    HiveParquetBlockReader::get_parquet_column_metadata(row_group, col.name());
                if let Ok(meta) = column_meta {
                    let in_memory_size = meta.uncompressed_size();
                    if let Ok(stats) = meta.statistics().transpose() {
                        // if stats is none, we could't make a decision wether the block should be filtered
                        let stats = match stats {
                            None => return false,
                            Some(stats) => stats,
                        };
                        if let (true, max, min, null_count) =
                            Self::get_max_min_stats(&col.data_type().into(), &*stats)
                        {
                            let col_stats = ColumnStatistics {
                                min,
                                max,
                                null_count: null_count as u64,
                                in_memory_size: in_memory_size as u64,
                                distinct_of_values: None,
                            };
                            if let Ok(idx) = self.data_schema.index_of(col.name()) {
                                statistics.insert(idx as u32, col_stats);
                            }
                        }
                    }
                }
            }

            for (p_key, p_value) in part_columns {
                if let Ok(idx) = self.data_schema.index_of(&p_key) {
                    let mut null_count = 0;
                    let v = if p_value == HIVE_DEFAULT_PARTITION {
                        null_count = row_group.num_rows();
                        Scalar::Null
                    } else {
                        Scalar::String(p_value.as_bytes().to_vec())
                    };

                    let col_stats = ColumnStatistics {
                        min: v.clone(),
                        max: v,
                        null_count: null_count as u64,
                        in_memory_size: 0,
                        distinct_of_values: None,
                    };
                    statistics.insert(idx as u32, col_stats);
                }
            }

            todo!("expression")
        }
        false
    }

    fn get_max_min_stats(
        column_type: &DataType,
        stats: &dyn Statistics,
    ) -> (bool, Scalar, Scalar, i64) {
        match column_type {
            DataType::Nullable(nullable_type) => {
                Self::get_max_min_stats(nullable_type.as_ref(), stats)
            }

            _ => todo!("expression"),
        }
    }
}
