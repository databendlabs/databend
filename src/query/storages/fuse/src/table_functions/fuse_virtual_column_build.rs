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

use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::VirtualColumnLayout;
use databend_common_catalog::plan::VirtualColumnPath;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::i64_value;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_args::u64_literal;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_storage::read_metadata_async;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use parquet::file::metadata::ParquetMetaDataReader;

use crate::FuseTable;
use crate::io::MetaReaders;
use crate::io::VirtualColumnBuilder;
use crate::io::VirtualColumnLayoutPolicy;
use crate::operations::column_parquet_metas;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::table_functions::fuse_virtual_column_parquet_meta::build_source_column_name_map;
use crate::table_functions::fuse_virtual_column_parquet_meta::virtual_column_file_variants;
use crate::table_functions::string_literal;

pub struct FuseVirtualColumnBuildArgs {
    database_name: String,
    table_name: String,
    location: String,
    max_direct_columns: Option<usize>,
}

impl TryFrom<(&str, TableArgs)> for FuseVirtualColumnBuildArgs {
    type Error = ErrorCode;

    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let args = table_args.expect_all_positioned(func_name, None)?;
        if !matches!(args.len(), 3 | 4) {
            return Err(ErrorCode::BadArguments(format!(
                "expecting <database_name>, <table_name>, (<segment_location> or <block_location>) and optional <max_direct_columns>, but got {:?}",
                args
            )));
        }
        let max_direct_columns = if let Some(value) = args.get(3) {
            let value = i64_value(value)?;
            if value < 0 {
                return Err(ErrorCode::BadArguments(
                    "max_direct_columns must be non-negative".to_string(),
                ));
            }
            Some(value as usize)
        } else {
            None
        };
        Ok(Self {
            database_name: string_value(&args[0])?,
            table_name: string_value(&args[1])?,
            location: string_value(&args[2])?,
            max_direct_columns,
        })
    }
}

impl From<&FuseVirtualColumnBuildArgs> for TableArgs {
    fn from(args: &FuseVirtualColumnBuildArgs) -> Self {
        let mut values = vec![
            string_literal(&args.database_name),
            string_literal(&args.table_name),
            string_literal(&args.location),
        ];
        if let Some(max_direct_columns) = args.max_direct_columns {
            values.push(u64_literal(max_direct_columns as u64));
        }
        TableArgs::new_positioned(values)
    }
}

pub type FuseVirtualColumnBuildFunc = SimpleArgFuncTemplate<FuseVirtualColumnBuild>;
pub struct FuseVirtualColumnBuild;

#[async_trait::async_trait]
impl SimpleArgFunc for FuseVirtualColumnBuild {
    type Args = FuseVirtualColumnBuildArgs;

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("block_location", TableDataType::String),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "virtual_column_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("direct_columns", TableDataType::Variant),
            TableField::new("shared_columns", TableDataType::Variant),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        _plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let table = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(&ctx.get_tenant(), &args.database_name, &args.table_name)
            .await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let (source_schema, projection) = variant_projection(table)?;
        let policy = args.max_direct_columns.map_or_else(
            || table.virtual_column_layout_policy(),
            |max_direct_columns| VirtualColumnLayoutPolicy {
                max_direct_columns,
                ..table.virtual_column_layout_policy()
            },
        );
        if args.location.ends_with(".mpk") {
            return build_segment_virtual_columns(
                ctx,
                table,
                &args.location,
                source_schema,
                projection,
                policy,
            )
            .await;
        }
        if !args.location.ends_with(".parquet") {
            return Err(ErrorCode::BadArguments(format!(
                "location must end with .parquet or .mpk: {}",
                args.location
            )));
        }
        let block = read_block_location(ctx, table, &args.location, projection).await?;
        let builder = VirtualColumnBuilder::try_create(source_schema, policy)?;
        build_block_virtual_columns(ctx, table, &args.location, builder, block)
    }
}

async fn build_segment_virtual_columns(
    ctx: &Arc<dyn TableContext>,
    table: &FuseTable,
    segment_location: &str,
    source_schema: TableSchemaRef,
    projection: Projection,
    policy: VirtualColumnLayoutPolicy,
) -> Result<DataBlock> {
    let reader = MetaReaders::segment_info_reader(table.get_operator(), table.schema());
    let segment = SegmentInfo::try_from(
        reader
            .read(&databend_storages_common_cache::LoadParams {
                location: segment_location.to_string(),
                len_hint: None,
                ver: SegmentInfo::VERSION,
                put_cache: false,
            })
            .await?
            .as_ref(),
    )?;
    let block_reader = table.create_block_reader(ctx.clone(), projection, false)?;
    let settings = ReadSettings::from_ctx(ctx)?;
    let storage_format = table.get_write_settings().storage_format;
    let mut blocks = Vec::with_capacity(segment.blocks.len());
    let mut path_counts =
        std::collections::HashMap::<u32, std::collections::HashMap<String, u64>>::new();
    for block_meta in &segment.blocks {
        let block = block_reader
            .read_by_meta(&settings, block_meta, &storage_format)
            .await?;
        let mut statistics =
            crate::io::JsonPathStatisticsBuilder::try_create(source_schema.clone(), policy)?;
        statistics.add_block(&block)?;
        for (source_id, source) in statistics.finalize() {
            for (path, count) in source.path_counts {
                *path_counts
                    .entry(source_id)
                    .or_default()
                    .entry(path)
                    .or_default() += count as u64;
            }
        }
        blocks.push((block_meta.location.0.clone(), block));
    }
    let mut direct_paths = Vec::new();
    let max_direct_columns = policy.max_direct_columns;
    let mut path_counts = path_counts.into_iter().collect::<Vec<_>>();
    path_counts.sort_unstable_by_key(|(source_column_id, _)| *source_column_id);
    for (source_column_id, counts) in path_counts {
        let mut counts = counts.into_iter().collect::<Vec<_>>();
        counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        direct_paths.extend(
            counts
                .into_iter()
                .take(max_direct_columns)
                .map(|(path, _)| VirtualColumnPath {
                    source_column_id,
                    path,
                }),
        );
    }
    direct_paths.sort();
    let layout = Arc::new(VirtualColumnLayout { direct_paths });
    let mut output = Vec::with_capacity(blocks.len());
    for (block_location, block) in blocks {
        let builder = VirtualColumnBuilder::try_create(source_schema.clone(), policy)?
            .with_adaptive_layout(layout.clone());
        output.push(build_block_virtual_columns(
            ctx,
            table,
            &block_location,
            builder,
            block,
        )?);
    }
    DataBlock::concat(&output)
}

fn build_block_virtual_columns(
    ctx: &Arc<dyn TableContext>,
    table: &FuseTable,
    block_location: &str,
    mut builder: VirtualColumnBuilder,
    block: DataBlock,
) -> Result<DataBlock> {
    builder.add_block(&block)?;
    let state = builder.finalize(
        &table.get_write_settings(),
        &(block_location.to_string(), 0),
    )?;
    if state.data.is_empty() {
        return Ok(DataBlock::empty_with_schema(
            &FuseVirtualColumnBuild::schema().into(),
        ));
    }
    let virtual_column_size = state
        .draft_virtual_block_meta
        .virtual_columns
        .as_ref()
        .map(|columns| columns.virtual_column_size)
        .unwrap_or_default();
    let parquet_meta = ParquetMetaDataReader::new().parse_and_finish(&state.data.to_bytes())?;
    let virtual_meta = VirtualColumnFileMeta::try_from(parquet_meta)?;
    let source_names = build_source_column_name_map(table.schema().as_ref());
    let func_ctx = ctx.get_function_context()?;
    let (direct_columns, shared_columns) =
        virtual_column_file_variants(&virtual_meta, &source_names, &func_ctx);
    Ok(DataBlock::new_from_columns(vec![
        StringType::from_data(vec![block_location.to_string()]),
        UInt64Type::from_data(vec![block.num_rows() as u64]),
        UInt64Type::from_data(vec![virtual_column_size]),
        VariantType::from_data(vec![direct_columns]),
        VariantType::from_data(vec![shared_columns]),
    ]))
}

fn variant_projection(table: &FuseTable) -> Result<(TableSchemaRef, Projection)> {
    let schema = table.schema();
    let mut fields = Vec::new();
    let mut indices = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if field.data_type().remove_nullable() == TableDataType::Variant
            && !matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_)))
        {
            fields.push(field.clone());
            indices.push(index);
        }
    }
    if fields.is_empty() {
        return Err(ErrorCode::VirtualColumnError(
            "Virtual column only supports tables with Variant fields",
        ));
    }
    let source_schema = Arc::new(TableSchema {
        fields,
        ..schema.as_ref().clone()
    });
    Ok((source_schema, Projection::Columns(indices)))
}

async fn read_block_location(
    ctx: &Arc<dyn TableContext>,
    table: &FuseTable,
    block_location: &str,
    projection: Projection,
) -> Result<DataBlock> {
    // Read the parquet footer directly from the user-provided location. This deliberately does
    // not consult snapshots or segments, so orphaned and historical blocks can also be inspected.
    let parquet_meta = read_metadata_async(block_location, &table.operator, None).await?;
    if parquet_meta.num_row_groups() != 1 {
        return Err(ErrorCode::ParquetFileInvalid(format!(
            "invalid Fuse block {}, expected one row group but got {}",
            block_location,
            parquet_meta.num_row_groups()
        )));
    }

    let physical_schema = Arc::new(table.schema_with_stream().remove_virtual_computed_fields());
    let column_metas = column_parquet_metas(&parquet_meta, &physical_schema)?;
    let row_group = &parquet_meta.row_groups()[0];
    let compression = row_group
        .columns()
        .first()
        .ok_or_else(|| {
            ErrorCode::ParquetFileInvalid(format!(
                "Fuse block {} has no parquet columns",
                block_location
            ))
        })?
        .compression()
        .try_into()?;
    let reader = table.create_block_reader(ctx.clone(), projection, false)?;
    let settings = ReadSettings::from_ctx(ctx)?;
    let data = reader
        .read_columns_data_by_merge_io(&settings, block_location, &column_metas, &None)
        .await?;
    let meta = BlockReadInfo {
        location: block_location.to_string(),
        row_count: row_group.num_rows() as u64,
        col_metas: column_metas,
        compression,
        block_size: 0,
    };
    reader.deserialize_chunks_with_meta(&meta, &table.storage_format, data)
}
