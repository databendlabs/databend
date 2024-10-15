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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;

use crate::meta::v2::BlockMeta;
use crate::meta::ColumnStatistics;
use crate::meta::FormatVersion;
use crate::meta::SegmentInfo;
use crate::meta::Statistics;

/// BlockMeta.col_stats is empty, and ColumnStatistics is stored in columnar_block_metas.
///
/// It is a minimal implementation for now, some other fields of block_metas(col_metas, cluster_stats) will be stored in columnar_block_metas later.
#[derive(Clone)]
pub struct ColumnarSegmentInfo {
    pub format_version: FormatVersion,
    pub summary: Statistics,
    pub block_metas: Vec<Arc<BlockMeta>>,
    pub columnar_block_metas: ColumnarBlockMeta,
}

#[derive(Clone)]
pub struct ColumnarBlockMeta {
    pub data: DataBlock,
    pub schema: TableSchemaRef,
}

const FIELDS_OF_COL_STATS: &[&str] = &[
    "min",
    "max",
    "null_count",
    "in_memory_size",
    "distinct_of_values",
];

impl ColumnarSegmentInfo {
    pub fn try_from_segment_info_and_schema(
        value: SegmentInfo,
        schema: &TableSchema,
    ) -> std::result::Result<Self, ErrorCode> {
        let block_metas = Self::build_lite_block_metas(&value.blocks)?;
        let columnar_block_metas = Self::block_metas_to_columnar(&block_metas, schema)?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary,
            block_metas,
            columnar_block_metas,
        })
    }

    fn build_lite_block_metas(blocks: &[Arc<BlockMeta>]) -> Result<Vec<Arc<BlockMeta>>> {
        let mut block_metas = Vec::with_capacity(blocks.len());
        for block in blocks {
            let new_block_meta = BlockMeta {
                col_stats: HashMap::new(),
                ..block.as_ref().clone()
            };
            block_metas.push(Arc::new(new_block_meta));
        }
        Ok(block_metas)
    }

    fn block_metas_to_columnar(
        blocks: &[Arc<BlockMeta>],
        table_schema: &TableSchema,
    ) -> Result<ColumnarBlockMeta> {
        let mut fields = Vec::with_capacity(table_schema.fields.len());
        let mut columns: Vec<Column> = Vec::with_capacity(table_schema.fields.len());

        for table_field in table_schema.fields.iter() {
            if !range_index_supported_type(&table_field.data_type) {
                continue;
            }

            fields.push(TableField::new(
                &table_field.column_id.to_string(),
                TableDataType::Nullable(Box::new(TableDataType::Tuple {
                    fields_name: FIELDS_OF_COL_STATS.iter().map(|s| s.to_string()).collect(),
                    fields_type: Self::types_of_col_stats(&table_field.data_type),
                })),
            ));

            let data_type = DataType::from(table_field.data_type());
            let mut nulls = MutableBitmap::with_capacity(blocks.len());
            let mut mins = ColumnBuilder::with_capacity(&data_type, blocks.len());
            let mut maxs = ColumnBuilder::with_capacity(&data_type, blocks.len());
            let mut null_counts = Vec::with_capacity(blocks.len());
            let mut in_memory_sizes = Vec::with_capacity(blocks.len());
            let mut distinct_of_values = Vec::with_capacity(blocks.len());
            for block in blocks {
                match block.col_stats.get(&table_field.column_id) {
                    Some(col_stats) => {
                        nulls.push(true);
                        mins.push(col_stats.min.as_ref());
                        maxs.push(col_stats.max.as_ref());
                        null_counts.push(col_stats.null_count);
                        in_memory_sizes.push(col_stats.in_memory_size);
                        distinct_of_values.push(col_stats.distinct_of_values);
                    }
                    None => {
                        nulls.push(false);
                        mins.push_default();
                        maxs.push_default();
                        null_counts.push(Default::default());
                        in_memory_sizes.push(Default::default());
                        distinct_of_values.push(Default::default());
                    }
                }
            }

            columns.push(Column::Nullable(Box::new(NullableColumn::new(
                Column::Tuple(vec![
                    mins.build(),
                    maxs.build(),
                    UInt64Type::from_data(null_counts),
                    UInt64Type::from_data(in_memory_sizes),
                    UInt64Type::from_opt_data(distinct_of_values),
                ]),
                Bitmap::from(nulls),
            ))));
        }

        let schema = Arc::new(TableSchema::new(fields));
        let data = DataBlock::new_from_columns(columns);
        Ok(ColumnarBlockMeta { data, schema })
    }

    fn types_of_col_stats(data_type: &TableDataType) -> Vec<TableDataType> {
        vec![
            data_type.clone(),
            data_type.clone(),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
        ]
    }

    pub fn to_lite_segment_info(&self) -> SegmentInfo {
        SegmentInfo {
            format_version: self.format_version,
            summary: self.summary.clone(),
            blocks: self.block_metas.clone(),
        }
    }

    pub fn col_stats(
        columnar_block_metas: ColumnarBlockMeta,
        block_id: usize,
    ) -> Result<HashMap<u32, ColumnStatistics>> {
        let mut col_stats = HashMap::with_capacity(columnar_block_metas.data.num_columns());
        for (block_entry, table_field) in columnar_block_metas
            .data
            .columns()
            .iter()
            .zip(columnar_block_metas.schema.fields())
        {
            let col_id = table_field.name.parse::<u32>().unwrap();
            let column = block_entry.to_column(columnar_block_metas.data.num_rows());
            let nullable_column = column.into_nullable().unwrap();
            match nullable_column.index(block_id) {
                Some(Some(scalar)) => {
                    let tuple = scalar.as_tuple().unwrap();
                    col_stats.insert(col_id, ColumnStatistics {
                        min: tuple[0].to_owned(),
                        max: tuple[1].to_owned(),
                        null_count: *tuple[2].as_number().unwrap().as_u_int64().unwrap(),
                        in_memory_size: *tuple[3].as_number().unwrap().as_u_int64().unwrap(),
                        distinct_of_values: tuple[4].as_number().map(|n| *n.as_u_int64().unwrap()),
                    });
                }
                Some(None) => {
                    continue;
                }
                None => unreachable!(),
            }
        }
        Ok(col_stats)
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
