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
use databend_common_column::bitmap::MutableBitmap;
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
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::VirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;
use crate::table_functions::fuse_block_statistics::build_variant;
use crate::table_functions::fuse_virtual_column_parquet_meta::build_source_column_name_map;

pub struct FuseVirtualColumnBlockMeta;
pub type FuseVirtualColumnBlockMetaFunc = TableMetaFuncTemplate<FuseVirtualColumnBlockMeta>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseVirtualColumnBlockMeta {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("segment_location", TableDataType::String),
            TableField::new("block_location", TableDataType::String),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("virtual_location", TableDataType::String.wrap_nullable()),
            TableField::new(
                "virtual_column_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "virtual_columns_complete",
                TableDataType::Boolean.wrap_nullable(),
            ),
            TableField::new(
                "virtual_column_metas",
                TableDataType::Variant.wrap_nullable(),
            ),
            TableField::new(
                "virtual_path_statistics",
                TableDataType::Variant.wrap_nullable(),
            ),
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
        let mut segment_locations = Vec::new();
        let mut block_locations = Vec::new();
        let mut row_counts = Vec::new();
        let mut virtual_locations = Vec::new();
        let mut virtual_column_sizes = Vec::new();
        let mut virtual_columns_complete = Vec::new();
        let mut virtual_column_metas = Vec::new();
        let mut virtual_path_statistics = Vec::new();
        let chunk_size = (ctx.get_settings().get_max_threads()? as usize * 4).max(1);

        'segments: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for (location, segment) in chunk.iter().zip(segments) {
                let segment = segment?;
                let path_map = segment
                    .summary
                    .virtual_segment_schema
                    .as_ref()
                    .map(virtual_segment_path_map)
                    .unwrap_or_default();
                for block in &segment.blocks {
                    segment_locations.push(location.0.clone());
                    block_locations.push(block.location.0.clone());
                    row_counts.push(block.row_count);

                    if let Some(virtual_block_meta) = &block.virtual_block_meta {
                        virtual_locations
                            .push(Some(virtual_block_meta.virtual_location.0.to_string()));
                        virtual_column_sizes.push(Some(virtual_block_meta.virtual_column_size));
                        virtual_columns_complete
                            .push(Some(virtual_block_meta.virtual_columns_complete));
                        let virtual_column_meta = build_virtual_column_metas(
                            &virtual_block_meta.virtual_column_metas,
                            &path_map,
                            &source_column_names,
                            &func_ctx,
                        );
                        virtual_column_metas.push(Some(virtual_column_meta));
                    } else {
                        virtual_locations.push(None);
                        virtual_column_sizes.push(None);
                        virtual_columns_complete.push(None);
                        virtual_column_metas.push(None);
                    }

                    if let Some(path_stat) = &block.virtual_path_statistics {
                        let virtual_path_statistic = build_virtual_path_statistic(
                            path_stat,
                            &path_map,
                            &source_column_names,
                            &func_ctx,
                        );
                        virtual_path_statistics.push(Some(virtual_path_statistic));
                    } else {
                        virtual_path_statistics.push(None);
                    }

                    if block_locations.len() >= limit {
                        break 'segments;
                    }
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(segment_locations),
            StringType::from_data(block_locations),
            UInt64Type::from_data(row_counts),
            StringType::from_opt_data(virtual_locations),
            UInt64Type::from_opt_data(virtual_column_sizes),
            BooleanType::from_opt_data(virtual_columns_complete),
            VariantType::from_opt_data(virtual_column_metas),
            VariantType::from_opt_data(virtual_path_statistics),
        ]))
    }
}

fn build_virtual_column_metas(
    virtual_column_metas: &HashMap<ColumnId, VirtualColumnMeta>,
    path_map: &HashMap<ColumnId, (ColumnId, &str)>,
    source_column_names: &HashMap<ColumnId, String>,
    func_ctx: &FunctionContext,
) -> Vec<u8> {
    let mut column_ids = Vec::with_capacity(virtual_column_metas.len());
    let mut path_names = Vec::with_capacity(virtual_column_metas.len());
    let mut offsets = Vec::with_capacity(virtual_column_metas.len());
    let mut lens = Vec::with_capacity(virtual_column_metas.len());
    let mut num_values = Vec::with_capacity(virtual_column_metas.len());
    let mut data_types = Vec::with_capacity(virtual_column_metas.len());
    let mut mins = Vec::with_capacity(virtual_column_metas.len());
    let mut maxes = Vec::with_capacity(virtual_column_metas.len());
    let mut null_counts = Vec::with_capacity(virtual_column_metas.len());
    let mut in_memory_sizes = Vec::with_capacity(virtual_column_metas.len());
    let mut column_stat_bitmap = MutableBitmap::with_capacity(virtual_column_metas.len());

    for (column_id, virtual_column_meta) in virtual_column_metas {
        let path_name = path_map
            .get(column_id)
            .map(|(source_column_id, path)| {
                let source_name = source_column_names
                    .get(source_column_id)
                    .cloned()
                    .unwrap_or_else(|| source_column_id.to_string());
                format!("{}.{}", &source_name, &path)
            })
            .unwrap_or_else(|| column_id.to_string());

        column_ids.push(*column_id);
        path_names.push(path_name);
        offsets.push(virtual_column_meta.offset);
        lens.push(virtual_column_meta.len);
        num_values.push(virtual_column_meta.num_values);

        let data_type = virtual_column_meta
            .physical_type()
            .table_data_type()
            .to_string();
        data_types.push(data_type);

        if let Some(column_stat) = &virtual_column_meta.column_stat {
            column_stat_bitmap.push(true);
            let min_type = infer_schema_type(&column_stat.min.as_ref().infer_data_type()).unwrap();
            let min_val = build_variant(column_stat.min.clone(), &min_type, func_ctx);
            mins.push(min_val);
            let max_type = infer_schema_type(&column_stat.max.as_ref().infer_data_type()).unwrap();
            let max_val = build_variant(column_stat.max.clone(), &max_type, func_ctx);
            maxes.push(max_val);
            null_counts.push(column_stat.null_count);
            in_memory_sizes.push(column_stat.in_memory_size);
        } else {
            column_stat_bitmap.push(false);
            mins.push(vec![]);
            maxes.push(vec![]);
            null_counts.push(0);
            in_memory_sizes.push(0);
        }
    }

    let virtual_column_metas_scalar = Scalar::Array(Column::Tuple(vec![
        UInt32Type::from_data(column_ids),
        StringType::from_data(path_names),
        UInt64Type::from_data(offsets),
        UInt64Type::from_data(lens),
        UInt64Type::from_data(num_values),
        StringType::from_data(data_types),
        Column::Nullable(Box::new(NullableColumn::new(
            Column::Tuple(vec![
                VariantType::from_data(mins),
                VariantType::from_data(maxes),
                UInt64Type::from_data(null_counts),
                UInt64Type::from_data(in_memory_sizes),
            ]),
            column_stat_bitmap.into(),
        ))),
    ]));

    let data_type = TableDataType::Array(Box::new(TableDataType::Tuple {
        fields_name: vec![
            "column_id".to_string(),
            "path".to_string(),
            "offset".to_string(),
            "len".to_string(),
            "num_values".to_string(),
            "data_type".to_string(),
            "column_stat".to_string(),
        ],
        fields_type: vec![
            TableDataType::Number(NumberDataType::UInt32),
            TableDataType::String,
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::String,
            TableDataType::Tuple {
                fields_name: vec![
                    "min".to_string(),
                    "max".to_string(),
                    "null_count".to_string(),
                    "in_memory_size".to_string(),
                ],
                fields_type: vec![
                    TableDataType::Variant,
                    TableDataType::Variant,
                    TableDataType::Number(NumberDataType::UInt64),
                    TableDataType::Number(NumberDataType::UInt64),
                ],
            }
            .wrap_nullable(),
        ],
    }));

    build_variant(virtual_column_metas_scalar, &data_type, func_ctx)
}

fn build_virtual_path_statistic(
    virtual_path_statistics: &HashMap<ColumnId, VirtualColumnPathStatistics>,
    path_map: &HashMap<ColumnId, (ColumnId, &str)>,
    source_column_names: &HashMap<ColumnId, String>,
    func_ctx: &FunctionContext,
) -> Vec<u8> {
    let mut column_ids = Vec::new();
    let mut path_names = Vec::new();
    let mut path_counts = Vec::new();

    for (source_column_id, path_stats) in virtual_path_statistics {
        let source_name = source_column_names
            .get(source_column_id)
            .cloned()
            .unwrap_or_else(|| source_column_id.to_string());
        for (column_id, path_count) in &path_stats.path_counts {
            let path_name = path_map
                .get(column_id)
                .map(|(_, path)| format!("{}.{}", &source_name, &path))
                .unwrap_or_else(|| column_id.to_string());

            column_ids.push(*column_id);
            path_names.push(path_name);
            path_counts.push(*path_count);
        }
    }

    let virtual_path_statistics_scalar = Scalar::Array(Column::Tuple(vec![
        UInt32Type::from_data(column_ids),
        StringType::from_data(path_names),
        UInt32Type::from_data(path_counts),
    ]));

    let data_type = TableDataType::Array(Box::new(TableDataType::Tuple {
        fields_name: vec![
            "column_id".to_string(),
            "path_name".to_string(),
            "path_count".to_string(),
        ],
        fields_type: vec![
            TableDataType::Number(NumberDataType::UInt32),
            TableDataType::String,
            TableDataType::Number(NumberDataType::UInt32),
        ],
    }));

    build_variant(virtual_path_statistics_scalar, &data_type, func_ctx)
}

fn virtual_segment_path_map(schema: &VirtualSegmentSchema) -> HashMap<ColumnId, (ColumnId, &str)> {
    schema
        .column_paths
        .iter()
        .flat_map(|source| {
            source.paths.iter().map(move |path| {
                (
                    path.column_id,
                    (source.source_column_id, path.path.as_str()),
                )
            })
        })
        .collect()
}
