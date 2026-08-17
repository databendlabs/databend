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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;
use crate::table_functions::fuse_block_statistics::build_variant;
use crate::table_functions::fuse_virtual_column_parquet_meta::build_source_column_name_map;

pub struct FuseVirtualColumnSegmentSchema;
pub type FuseVirtualColumnSegmentSchemaFunc = TableMetaFuncTemplate<FuseVirtualColumnSegmentSchema>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseVirtualColumnSegmentSchema {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("segment_location", TableDataType::String),
            TableField::new("block_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "virtual_column_count",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "virtual_column_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new("virtual_schema", TableDataType::Variant.wrap_nullable()),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let source_column_names = build_source_column_name_map(tbl.schema().as_ref());
        let func_ctx = ctx.get_function_context()?;
        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());
        let capacity = limit.min(snapshot.segments.len());
        let mut segment_locations = Vec::with_capacity(capacity);
        let mut block_counts = Vec::with_capacity(capacity);
        let mut row_counts = Vec::with_capacity(capacity);
        let mut virtual_block_counts = Vec::with_capacity(capacity);
        let mut virtual_column_sizes = Vec::with_capacity(capacity);
        let mut virtual_schemas = Vec::with_capacity(capacity);
        let chunk_size = (ctx.get_settings().get_max_threads()? as usize * 4).max(1);

        'segments: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for (location, segment) in chunk.iter().zip(segments) {
                let segment = segment?;
                let virtual_schema = segment
                    .summary
                    .virtual_segment_schema
                    .as_ref()
                    .map(|schema| build_segment_schema(schema, &source_column_names, &func_ctx));

                segment_locations.push(location.0.clone());
                block_counts.push(segment.summary.block_count);
                row_counts.push(segment.summary.row_count);
                virtual_block_counts.push(segment.summary.virtual_block_count);
                virtual_column_sizes.push(segment.summary.virtual_column_size);
                virtual_schemas.push(virtual_schema);
                if segment_locations.len() >= limit {
                    break 'segments;
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(segment_locations),
            UInt64Type::from_data(block_counts),
            UInt64Type::from_data(row_counts),
            UInt64Type::from_opt_data(virtual_block_counts),
            UInt64Type::from_opt_data(virtual_column_sizes),
            VariantType::from_opt_data(virtual_schemas),
        ]))
    }
}

fn build_segment_schema(
    schema: &VirtualSegmentSchema,
    source_column_names: &HashMap<ColumnId, String>,
    func_ctx: &FunctionContext,
) -> Vec<u8> {
    let mut column_ids = Vec::new();
    let mut path_names = Vec::new();
    for column_paths in &schema.column_paths {
        let source_name = source_column_names
            .get(&column_paths.source_column_id)
            .cloned()
            .unwrap_or_else(|| column_paths.source_column_id.to_string());
        for path in &column_paths.paths {
            column_ids.push(path.column_id);
            let path_name = format!("{}.{}", &source_name, &path.path);
            path_names.push(path_name);
        }
    }
    let virtual_fields = Scalar::Array(Column::Tuple(vec![
        UInt32Type::from_data(column_ids),
        StringType::from_data(path_names),
    ]));
    let data_type = TableDataType::Array(Box::new(TableDataType::Tuple {
        fields_name: vec!["column_id".to_string(), "path".to_string()],
        fields_type: vec![
            TableDataType::Number(NumberDataType::UInt32),
            TableDataType::String,
        ],
    }));

    build_variant(virtual_fields, &data_type, func_ctx)
}
