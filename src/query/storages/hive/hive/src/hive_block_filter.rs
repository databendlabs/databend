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
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_storages_index::RangeFilter;
use common_storages_table_meta::meta::ColumnStatistics;
use common_storages_table_meta::meta::StatisticsOfColumns;

use crate::hive_parquet_block_reader::HiveBlockReader;
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
                    HiveBlockReader::get_parquet_column_metadata(row_group, col.name());
                if let Ok(meta) = column_meta {
                    let in_memory_size = meta.uncompressed_size();
                    if let Ok(stats) = meta.statistics().transpose() {
                        // if stats is none, we could't make a decision wether the block should be filtered
                        let stats = match stats {
                            None => return false,
                            Some(stats) => stats,
                        };
                        if let (true, max, min, null_count) =
                            Self::get_max_min_stats(col.data_type(), &*stats)
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
                        DataValue::Null
                    } else {
                        DataValue::String(p_value.as_bytes().to_vec())
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

            if let Ok(ret) = filter.eval(&statistics, row_group.num_rows() as u64) {
                if !ret {
                    return true;
                }
            }
        }
        false
    }

    fn get_max_min_stats(
        column_type: &DataTypeImpl,
        stats: &dyn Statistics,
    ) -> (bool, DataValue, DataValue, i64) {
        match column_type {
            DataTypeImpl::Nullable(nullable_type) => {
                Self::get_max_min_stats(nullable_type.inner_type(), stats)
            }
            DataTypeImpl::Boolean(_) => {
                let s = stats.as_any().downcast_ref::<BooleanStatistics>().unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap());
                    let min = DataValue::from(s.min_value.unwrap());
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::Int8(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap() as i8);
                    let min = DataValue::from(s.min_value.unwrap() as i8);
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::Int16(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap() as i16);
                    let min = DataValue::from(s.min_value.unwrap() as i16);
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::Int32(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap());
                    let min = DataValue::from(s.min_value.unwrap());
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::Int64(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap());
                    let min = DataValue::from(s.min_value.unwrap());
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::UInt8(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap() as u8);
                    let min = DataValue::from(s.min_value.unwrap() as u8);
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::UInt16(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap() as u16);
                    let min = DataValue::from(s.min_value.unwrap() as u16);
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::UInt32(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap() as u32);
                    let min = DataValue::from(s.min_value.unwrap() as u32);
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::UInt64(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap() as u64);
                    let min = DataValue::from(s.min_value.unwrap() as u64);
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::Float32(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<f32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap());
                    let min = DataValue::from(s.min_value.unwrap());
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::Float64(_) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<f64>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.unwrap());
                    let min = DataValue::from(s.min_value.unwrap());
                    (true, max, min, null_count)
                }
            }
            DataTypeImpl::String(_) => {
                let s = stats.as_any().downcast_ref::<BinaryStatistics>().unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    (false, DataValue::from(0), DataValue::from(0), 0)
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = DataValue::from(s.max_value.clone());
                    let min = DataValue::from(s.min_value.clone());
                    (true, max, min, null_count)
                }
            }
            _ => (false, DataValue::from(0), DataValue::from(0), 0),
        }
    }
}
