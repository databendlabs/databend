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

use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::statistics::BinaryStatistics;
use common_arrow::parquet::statistics::BooleanStatistics;
use common_arrow::parquet::statistics::PrimitiveStatistics;
use common_arrow::parquet::statistics::Statistics;
use common_expression::types::number::F32;
use common_expression::types::number::F64;
use common_expression::types::BooleanType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use storages_common_index::RangeIndex;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::hive_parquet_block_reader::HiveBlockReader;
use crate::hive_table::HIVE_DEFAULT_PARTITION;

#[derive(Clone)]
pub struct HiveBlockFilter {
    range_filter: Option<RangeIndex>,
    projections: Vec<TableField>,
    data_schema: Arc<TableSchema>,
}

impl HiveBlockFilter {
    pub fn create(
        range_filter: Option<RangeIndex>,
        projections: Vec<TableField>,
        data_schema: Arc<TableSchema>,
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
                        // if stats is none, we couldn't make a decision whether the block should be filtered
                        let stats = match stats {
                            None => return false,
                            Some(stats) => stats,
                        };
                        if let Some((max, min, null_count)) =
                            Self::get_max_min_stats(col.data_type(), &*stats)
                        {
                            let col_stats = ColumnStatistics::new(
                                min,
                                max,
                                null_count as u64,
                                in_memory_size as u64,
                                None,
                            );
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

                    let col_stats = ColumnStatistics::new(v.clone(), v, null_count as u64, 0, None);
                    statistics.insert(idx as u32, col_stats);
                }
            }

            if let Ok(ret) = filter.apply(&statistics, |_| false) {
                if !ret {
                    return true;
                }
            }
        }
        false
    }

    fn get_max_min_stats(
        column_type: &TableDataType,
        stats: &dyn Statistics,
    ) -> Option<(Scalar, Scalar, i64)> {
        match column_type {
            TableDataType::Number(NumberDataType::UInt8) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<u8>::upcast_scalar(s.max_value.unwrap() as u8);
                    let min = NumberType::<u8>::upcast_scalar(s.min_value.unwrap() as u8);
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::UInt16) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<u16>::upcast_scalar(s.max_value.unwrap() as u16);
                    let min = NumberType::<u16>::upcast_scalar(s.min_value.unwrap() as u16);
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::UInt32) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<u32>::upcast_scalar(s.max_value.unwrap() as u32);
                    let min = NumberType::<u32>::upcast_scalar(s.min_value.unwrap() as u32);
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::UInt64) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<u64>::upcast_scalar(s.max_value.unwrap() as u64);
                    let min = NumberType::<u64>::upcast_scalar(s.min_value.unwrap() as u64);
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::Int8) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<i8>::upcast_scalar(s.max_value.unwrap() as i8);
                    let min = NumberType::<i8>::upcast_scalar(s.min_value.unwrap() as i8);
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::Int16) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<i16>::upcast_scalar(s.max_value.unwrap() as i16);
                    let min = NumberType::<i16>::upcast_scalar(s.min_value.unwrap() as i16);
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::Int32) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<i32>::upcast_scalar(s.max_value.unwrap());
                    let min = NumberType::<i32>::upcast_scalar(s.min_value.unwrap());
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::Int64) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<i64>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<i64>::upcast_scalar(s.max_value.unwrap());
                    let min = NumberType::<i64>::upcast_scalar(s.min_value.unwrap());
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::Float32) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<f32>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<F32>::upcast_scalar(s.max_value.unwrap().into());
                    let min = NumberType::<F32>::upcast_scalar(s.min_value.unwrap().into());
                    Some((max, min, null_count))
                }
            }
            TableDataType::Number(NumberDataType::Float64) => {
                let s = stats
                    .as_any()
                    .downcast_ref::<PrimitiveStatistics<f64>>()
                    .unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = NumberType::<F64>::upcast_scalar(s.max_value.unwrap().into());
                    let min = NumberType::<F64>::upcast_scalar(s.min_value.unwrap().into());
                    Some((max, min, null_count))
                }
            }
            TableDataType::Boolean => {
                let s = stats.as_any().downcast_ref::<BooleanStatistics>().unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = BooleanType::upcast_scalar(s.max_value.unwrap());
                    let min = BooleanType::upcast_scalar(s.min_value.unwrap());
                    Some((max, min, null_count))
                }
            }
            TableDataType::String => {
                let s = stats.as_any().downcast_ref::<BinaryStatistics>().unwrap();
                if s.null_count.is_none() || s.max_value.is_none() || s.min_value.is_none() {
                    None
                } else {
                    let null_count = s.null_count.unwrap();
                    let max = StringType::upcast_scalar(s.max_value.clone().unwrap());
                    let min = StringType::upcast_scalar(s.min_value.clone().unwrap());
                    Some((max, min, null_count))
                }
            }
            TableDataType::Nullable(inner_ty) => Self::get_max_min_stats(inner_ty.as_ref(), stats),
            _ => None,
        }
    }
}
