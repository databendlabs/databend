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

use arrow::array::Array;
use arrow::array::AsArray;
use arrow::array::Date32Array;
use arrow::array::Decimal128Array;
use arrow::array::Decimal256Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::LargeStringArray;
use arrow::array::RecordBatch;
use arrow::array::StructArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::table_type_to_arrow_type;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use serde::Deserialize;
use serde::Serialize;

use super::super::v2;
use crate::meta::format::decode_segment_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::MetaCompression;
use crate::meta::format::SegmentHeader;
use crate::meta::format::MAX_SEGMENT_BLOCK_NUMBER;
use crate::meta::v2::BlockMeta;
use crate::meta::ColumnMeta;
use crate::meta::ColumnStatistics;
use crate::meta::FormatVersion;
use crate::meta::MetaEncoding;
use crate::meta::SegmentInfo;
use crate::meta::Statistics;
use crate::meta::Versioned;

/// BlockMeta.col_stats is empty, and ColumnStatistics is stored in columnar_block_metas.
///
/// It is a minimal implemention for now, some other fields of block_metas(col_metas, cluster_stats) will be stored in columnar_metas later.
pub struct ColumnarSegmentInfo {
    pub format_version: FormatVersion,
    pub summary: Statistics,
    pub block_metas: Vec<Arc<BlockMeta>>,
    pub columnar_block_metas: RecordBatch,
}

impl ColumnarSegmentInfo {
    pub fn try_from_segment_info_and_schema(
        value: SegmentInfo,
        schema: &TableSchema,
    ) -> std::result::Result<Self, ErrorCode> {
        let (block_metas, columnar_block_metas) =
            Self::block_metas_to_columnar(&value.blocks, schema)?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary,
            block_metas,
            columnar_block_metas,
        })
    }

    fn build_sub_fields_of_col_stats(data_type: &TableDataType) -> Fields {
        vec![
            Field::new("min", table_type_to_arrow_type(data_type), false),
            Field::new("max", table_type_to_arrow_type(data_type), false),
            Field::new("null_count", ArrowDataType::UInt64, false),
            Field::new("in_memory_size", ArrowDataType::UInt64, false),
            Field::new("distinct_of_values", ArrowDataType::UInt64, true),
        ]
        .into()
    }

    fn build_sub_arrays_of_col_stats(
        blocks: &[Arc<BlockMeta>],
        table_field: &TableField,
    ) -> Result<Vec<Arc<dyn Array>>> {
        let null_count: Vec<u64> =
            Self::col_stat_or_default(blocks, table_field.column_id, |s| s.null_count);
        let in_memory_size: Vec<u64> =
            Self::col_stat_or_default(blocks, table_field.column_id, |s| s.in_memory_size);
        let distinct_of_values: Vec<Option<u64>> =
            Self::col_stat_or_default(blocks, table_field.column_id, |s| s.distinct_of_values);
        let min = scalar_iter_to_array(
            blocks
                .iter()
                .map(|b| b.col_stats.get(&table_field.column_id).map(|s| &s.min)),
            &table_field.data_type,
        )?;
        let max = scalar_iter_to_array(
            blocks
                .iter()
                .map(|b| b.col_stats.get(&table_field.column_id).map(|s| &s.max)),
            &table_field.data_type,
        )?;
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(min),
            Arc::new(max),
            Arc::new(UInt64Array::from(null_count)),
            Arc::new(UInt64Array::from(in_memory_size)),
            Arc::new(UInt64Array::from(distinct_of_values)),
        ];
        Ok(arrays)
    }

    fn col_stat_or_default<T: Default>(
        blocks: &[Arc<BlockMeta>],
        col_id: u32,
        which_stat: impl Fn(&ColumnStatistics) -> T + Copy,
    ) -> Vec<T> {
        blocks
            .iter()
            .map(|b| b.col_stats.get(&col_id).map(which_stat).unwrap_or_default())
            .collect()
    }

    fn block_metas_to_columnar(
        blocks: &[Arc<BlockMeta>],
        table_schema: &TableSchema,
    ) -> Result<(Vec<Arc<BlockMeta>>, RecordBatch)> {
        let mut fields = Vec::with_capacity(table_schema.fields.len());
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(table_schema.fields.len());

        for table_field in table_schema.fields.iter() {
            if !range_index_supported_type(&table_field.data_type) {
                continue;
            }

            let sub_fields_of_col_stats =
                Self::build_sub_fields_of_col_stats(&table_field.data_type);
            fields.push(Field::new(
                table_field.column_id.to_string(),
                ArrowDataType::Struct(sub_fields_of_col_stats.clone()),
                true, // alter table add column will result in the old block metas having no col_stats of the new column
            ));

            let sub_arrays_of_col_stats = Self::build_sub_arrays_of_col_stats(blocks, table_field)?;

            let nulls = blocks
                .iter()
                .map(|b| b.col_stats.contains_key(&table_field.column_id))
                .collect::<Vec<_>>();

            columns.push(Arc::new(StructArray::try_new(
                sub_fields_of_col_stats,
                sub_arrays_of_col_stats,
                Some(NullBuffer::from(nulls.clone())),
            )?));
        }

        let schema = Schema::new(fields);
        let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

        let block_metas = blocks
            .iter()
            .map(|b| {
                let mut new_block_meta = b.as_ref().clone();
                new_block_meta.col_stats = HashMap::new();
                Arc::new(new_block_meta)
            })
            .collect();
        Ok((block_metas, record_batch))
    }
}

fn scalar_iter_to_array<'a, I>(iter: I, data_type: &TableDataType) -> Result<Arc<dyn Array>>
where I: Iterator<Item = Option<&'a Scalar>> {
    match data_type.remove_nullable() {
        TableDataType::Number(NumberDataType::Int8) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int8().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int8Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Int16) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int16().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int16Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Int32) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int32().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int32Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Int64) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int64().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int64Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt8) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int8().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt8Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt16) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int16().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt16Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt32) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int32().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt32Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt64) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int64().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt64Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Float32) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_float32().unwrap().as_ref())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Float32Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Float64) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_float64().unwrap().as_ref())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Float64Array::from(values)))
        }
        TableDataType::Date => {
            let values = iter
                .map(|s| s.map(|s| *s.as_date().unwrap()).unwrap_or_default())
                .collect::<Vec<_>>();
            Ok(Arc::new(Date32Array::from(values)))
        }
        TableDataType::Timestamp => {
            let values = iter
                .map(|s| s.map(|s| *s.as_timestamp().unwrap()).unwrap_or_default())
                .collect::<Vec<_>>();
            Ok(Arc::new(TimestampMicrosecondArray::from(values)))
        }
        TableDataType::String => {
            let values = iter
                .map(|s| {
                    s.map(|s| s.as_string().unwrap().to_string())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(LargeStringArray::from(values)))
        }
        TableDataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_decimal().unwrap().as_decimal128().unwrap().0)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Decimal128Array::from(values)))
        }
        TableDataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let values = iter
                .map(|s| unsafe {
                    s.map(|s| {
                        std::mem::transmute::<_, arrow::datatypes::i256>(
                            *s.as_decimal().unwrap().as_decimal256().unwrap().0,
                        )
                    })
                    .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Decimal256Array::from(values)))
        }
        _ => unreachable!(),
    }
}

fn range_index_supported_type(data_type: &TableDataType) -> bool {
    let inner_type = data_type.remove_nullable();
    matches!(
        inner_type,
        TableDataType::Number(_)
            | TableDataType::Date
            | TableDataType::Timestamp
            | TableDataType::String
            | TableDataType::Decimal(_)
    )
}
