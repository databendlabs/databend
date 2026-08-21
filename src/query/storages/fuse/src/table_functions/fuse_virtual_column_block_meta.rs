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

use databend_common_exception::Result;
use databend_common_catalog::table::Table;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;

pub struct FuseVirtualColumnBlockMeta;
pub type FuseVirtualColumnBlockMetaFunc = TableMetaFuncTemplate<FuseVirtualColumnBlockMeta>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseVirtualColumnBlockMeta {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("segment_location", TableDataType::String),
            TableField::new("block_location", TableDataType::String),
            TableField::new("virtual_location", TableDataType::String.wrap_nullable()),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("virtual_column_size", TableDataType::Number(NumberDataType::UInt64).wrap_nullable()),
            TableField::new("source_column_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("source_column_name", TableDataType::String),
            TableField::new("path", TableDataType::String),
            TableField::new("path_index", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("storage_kind", TableDataType::String),
            TableField::new("column_id", TableDataType::Number(NumberDataType::UInt32).wrap_nullable()),
            TableField::new("column_types", TableDataType::String),
            TableField::new("value_count", TableDataType::Number(NumberDataType::UInt64).wrap_nullable()),
            TableField::new("block_offset", TableDataType::Number(NumberDataType::UInt64).wrap_nullable()),
            TableField::new("bytes_compressed", TableDataType::Number(NumberDataType::UInt64).wrap_nullable()),
            TableField::new("num_values", TableDataType::Number(NumberDataType::UInt64).wrap_nullable()),
            TableField::new("path_statistics_complete", TableDataType::Boolean),
            TableField::new("virtual_columns_complete", TableDataType::Boolean),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let source_names = tbl
            .schema()
            .fields()
            .iter()
            .map(|field| (field.column_id(), field.name().to_string()))
            .collect::<HashMap<_, _>>();
        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());

        let mut segment_location = StringColumnBuilder::with_capacity(limit.min(1024));
        let mut block_location = StringColumnBuilder::with_capacity(limit.min(1024));
        let mut virtual_location = Vec::new();
        let mut row_count = Vec::new();
        let mut virtual_column_size = Vec::new();
        let mut source_column_id = Vec::new();
        let mut source_column_name = StringColumnBuilder::with_capacity(limit.min(1024));
        let mut path = StringColumnBuilder::with_capacity(limit.min(1024));
        let mut path_index = Vec::new();
        let mut storage_kind = StringColumnBuilder::with_capacity(limit.min(1024));
        let mut column_id = Vec::new();
        let mut column_types = StringColumnBuilder::with_capacity(limit.min(1024));
        let mut value_count = Vec::new();
        let mut block_offset = Vec::new();
        let mut bytes_compressed = Vec::new();
        let mut num_values = Vec::new();
        let mut path_statistics_complete = Vec::new();
        let mut virtual_columns_complete = Vec::new();
        let mut rows = 0usize;

        'segments: for location in &snapshot.segments {
            let segments = segments_io
                .read_segments::<databend_storages_common_table_meta::meta::SegmentInfo>(
                    std::slice::from_ref(location),
                    true,
                )
                .await?;
            let segment = segments.into_iter().next().unwrap()?;
            let Some(schema) = &segment.summary.virtual_segment_schema else {
                continue;
            };
            for block in &segment.blocks {
                let Some(meta) = &block.virtual_block_meta else {
                    continue;
                };
                for source in &schema.column_paths {
                    let counts = meta
                        .path_statistics
                        .iter()
                        .find(|stats| stats.source_column_id == source.source_column_id);
                    for (index, item) in source.paths.iter().enumerate() {
                        let physical = item
                            .column_id
                            .and_then(|id| meta.virtual_column_metas.get(&id).map(|column| (id, column)));
                        let count = counts.and_then(|stats| {
                            stats.paths.iter().find(|count| count.path_index as usize == index)
                        });
                        if physical.is_none() && count.is_none() {
                            continue;
                        }
                        segment_location.put_and_commit(&location.0);
                        block_location.put_and_commit(&block.location.0);
                        virtual_location.push(Some(meta.virtual_location.0.clone()));
                        row_count.push(block.row_count);
                        virtual_column_size.push(Some(meta.virtual_column_size));
                        source_column_id.push(source.source_column_id);
                        source_column_name.put_and_commit(
                            source_names
                                .get(&source.source_column_id)
                                .map(String::as_str)
                                .unwrap_or(""),
                        );
                        path.put_and_commit(&item.path);
                        path_index.push(index as u32);
                        storage_kind.put_and_commit(if item.column_id.is_some() { "direct" } else { "shared_candidate" });
                        column_id.push(item.column_id);
                        column_types.put_and_commit(
                            &item
                                .data_types
                                .iter()
                                .map(|data_type| format!("{data_type:?}"))
                                .collect::<Vec<_>>()
                                .join(","),
                        );
                        value_count.push(count.map(|count| count.value_count));
                        block_offset.push(physical.map(|(_, column)| column.offset));
                        bytes_compressed.push(physical.map(|(_, column)| column.len));
                        num_values.push(physical.map(|(_, column)| column.num_values));
                        path_statistics_complete.push(counts.is_some_and(|stats| stats.path_statistics_complete));
                        virtual_columns_complete.push(meta.virtual_columns_complete);
                        rows += 1;
                        if rows >= limit {
                            break 'segments;
                        }
                    }
                }
            }
        }

        Ok(DataBlock::new(vec![
            Column::String(segment_location.build()).into(),
            Column::String(block_location.build()).into(),
            StringType::from_opt_data(virtual_location).into(),
            UInt64Type::from_data(row_count).into(),
            UInt64Type::from_opt_data(virtual_column_size).into(),
            UInt32Type::from_data(source_column_id).into(),
            Column::String(source_column_name.build()).into(),
            Column::String(path.build()).into(),
            UInt32Type::from_data(path_index).into(),
            Column::String(storage_kind.build()).into(),
            UInt32Type::from_opt_data(column_id).into(),
            Column::String(column_types.build()).into(),
            UInt64Type::from_opt_data(value_count).into(),
            UInt64Type::from_opt_data(block_offset).into(),
            UInt64Type::from_opt_data(bytes_compressed).into(),
            UInt64Type::from_opt_data(num_values).into(),
            BooleanType::from_data(path_statistics_complete).into(),
            BooleanType::from_data(virtual_columns_complete).into(),
        ], rows))
    }
}
