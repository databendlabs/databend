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
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
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
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_storage::read_metadata_async;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use parquet::file::metadata::ParquetMetaDataReader;

use crate::FuseTable;
use crate::io::VirtualColumnBuilder;
use crate::operations::column_parquet_metas;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::table_functions::fuse_virtual_column::build_source_column_name_map;
use crate::table_functions::fuse_virtual_column::collect_virtual_column_entries;
use crate::table_functions::string_literal;

pub struct FuseVirtualColumnBuildArgs {
    database_name: String,
    table_name: String,
    block_location: String,
}

impl TryFrom<(&str, TableArgs)> for FuseVirtualColumnBuildArgs {
    type Error = ErrorCode;

    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let args = table_args.expect_all_positioned(func_name, Some(3))?;
        Ok(Self {
            database_name: string_value(&args[0])?,
            table_name: string_value(&args[1])?,
            block_location: string_value(&args[2])?,
        })
    }
}

impl From<&FuseVirtualColumnBuildArgs> for TableArgs {
    fn from(args: &FuseVirtualColumnBuildArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(&args.database_name),
            string_literal(&args.table_name),
            string_literal(&args.block_location),
        ])
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
            TableField::new(
                "source_column_id",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new("source_column_name", TableDataType::String),
            TableField::new("storage_kind", TableDataType::String),
            TableField::new("shared_paths", TableDataType::String.wrap_nullable()),
            TableField::new("column_name", TableDataType::String),
            TableField::new("column_type", TableDataType::String),
            TableField::new(
                "column_id",
                TableDataType::Number(NumberDataType::UInt32).wrap_nullable(),
            ),
            TableField::new("num_values", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "block_offset",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
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
        let block = read_block_location(ctx, table, &args.block_location, projection).await?;

        let mut builder = VirtualColumnBuilder::try_create(source_schema)?;
        builder.add_block(&block)?;
        let state = builder.finalize(
            &table.get_write_settings(),
            &(args.block_location.clone(), 0),
        )?;
        if state.data.is_empty() {
            return Ok(DataBlock::empty_with_schema(&Self::schema().into()));
        }

        let bytes = state.data.to_bytes();
        let parquet_meta = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
        let virtual_meta = VirtualColumnFileMeta::try_from(parquet_meta)?;
        let source_names = build_source_column_name_map(table.schema().as_ref());
        let entries = collect_virtual_column_entries(&virtual_meta, &source_names);
        Ok(entries_to_block(&args.block_location, entries))
    }
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

fn entries_to_block(
    block_location_value: &str,
    entries: Vec<super::fuse_virtual_column::VirtualColumnEntry>,
) -> DataBlock {
    let len = entries.len();
    let mut block_location = StringColumnBuilder::with_capacity(len);
    let mut source_column_ids = Vec::with_capacity(len);
    let mut source_column_names = StringColumnBuilder::with_capacity(len);
    let mut storage_kinds = StringColumnBuilder::with_capacity(len);
    let mut shared_paths = Vec::with_capacity(len);
    let mut column_names = StringColumnBuilder::with_capacity(len);
    let mut column_types = StringColumnBuilder::with_capacity(len);
    let mut column_ids = Vec::with_capacity(len);
    let mut num_values = Vec::with_capacity(len);
    let mut offsets = Vec::with_capacity(len);
    let mut compressed_bytes = Vec::with_capacity(len);

    for entry in entries {
        block_location.put_and_commit(block_location_value);
        source_column_ids.push(entry.source_column_id);
        source_column_names.put_and_commit(&entry.source_column_name);
        storage_kinds.put_and_commit(entry.storage_kind);
        shared_paths.push(entry.shared_paths);
        column_names.put_and_commit(&entry.column_name);
        column_types.put_and_commit(&entry.column_type);
        column_ids.push(entry.column_id);
        num_values.push(entry.num_values);
        offsets.push(entry.offset);
        compressed_bytes.push(entry.len);
    }

    DataBlock::new(
        vec![
            Column::String(block_location.build()).into(),
            UInt32Type::from_data(source_column_ids).into(),
            Column::String(source_column_names.build()).into(),
            Column::String(storage_kinds.build()).into(),
            StringType::from_opt_data(shared_paths).into(),
            Column::String(column_names.build()).into(),
            Column::String(column_types.build()).into(),
            UInt32Type::from_opt_data(column_ids).into(),
            UInt64Type::from_data(num_values).into(),
            UInt64Type::from_opt_data(offsets).into(),
            UInt64Type::from_opt_data(compressed_bytes).into(),
        ],
        len,
    )
}
