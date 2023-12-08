// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_functions::BUILTIN_FUNCTIONS;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_sql::parse_computed_expr;
use common_storages_fuse::io::serialize_block;
use common_storages_fuse::io::write_data;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::io::ReadSettings;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::io::WriteSettings;
use common_storages_fuse::FuseTable;
use opendal::Operator;
use storages_common_cache::LoadParams;

#[async_backtrace::framed]
pub async fn do_refresh_virtual_column(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    virtual_columns: Vec<String>,
) -> Result<()> {
    if virtual_columns.is_empty() {
        return Ok(());
    }

    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = if let Some(val) = snapshot_opt {
        val
    } else {
        // no snapshot
        return Ok(());
    };

    let table_schema = &fuse_table.get_table_info().meta.schema;

    let mut field_indices = Vec::new();
    for (i, f) in table_schema.fields().iter().enumerate() {
        if f.data_type().remove_nullable() != TableDataType::Variant {
            continue;
        }
        let mut is_src_column = false;
        for virtual_column in &virtual_columns {
            if virtual_column.starts_with(&f.name().clone()) {
                is_src_column = true;
            }
        }
        if is_src_column {
            field_indices.push(i);
        }
    }

    if field_indices.is_empty() {
        // no source variant column
        return Ok(());
    }
    let projected_schema = table_schema.project(&field_indices);
    let source_schema = Arc::new(DataSchema::from(&projected_schema));

    let projection = Projection::Columns(field_indices);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    let settings = ReadSettings::from_ctx(&ctx)?;
    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let operator = fuse_table.get_operator_ref();

    for (location, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;

        let block_metas = segment_info.block_metas()?;
        for block_meta in block_metas {
            let block = block_reader
                .read_by_meta(&settings, &block_meta, &storage_format)
                .await?;
            let virtual_loc =
                TableMetaLocationGenerator::gen_virtual_block_location(&block_meta.location.0);

            materialize_virtual_columns(
                ctx.clone(),
                operator,
                &write_settings,
                &virtual_loc,
                source_schema.clone(),
                &virtual_columns,
                block,
            )
            .await?;
        }
    }

    Ok(())
}

#[async_backtrace::framed]
async fn materialize_virtual_columns(
    ctx: Arc<dyn TableContext>,
    operator: &Operator,
    write_settings: &WriteSettings,
    location: &str,
    source_schema: DataSchemaRef,
    paths: &Vec<String>,
    block: DataBlock,
) -> Result<()> {
    let len = block.num_rows();

    let func_ctx = ctx.get_function_context()?;
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let mut virtual_fields = Vec::with_capacity(paths.len());
    let mut virtual_columns = Vec::with_capacity(paths.len());
    for path in paths {
        let expr = parse_computed_expr(ctx.clone(), source_schema.clone(), path)?;
        let value = evaluator.run(&expr)?;
        let virtual_field = TableField::new(
            path,
            TableDataType::Nullable(Box::new(TableDataType::Variant)),
        );
        virtual_fields.push(virtual_field);

        let virtual_column =
            BlockEntry::new(DataType::Nullable(Box::new(DataType::Variant)), value);
        virtual_columns.push(virtual_column);
    }
    let virtual_schema = TableSchemaRefExt::create(virtual_fields);
    let virtual_block = DataBlock::new(virtual_columns, len);

    let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
    let _ = serialize_block(write_settings, &virtual_schema, virtual_block, &mut buffer)?;

    write_data(buffer, operator, location).await?;

    Ok(())
}
