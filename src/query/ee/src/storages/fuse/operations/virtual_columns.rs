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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::VariantType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::ScalarRef;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_storages_fuse::io::serialize_block;
use common_storages_fuse::io::write_data;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::io::ReadSettings;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::io::WriteSettings;
use common_storages_fuse::FuseTable;
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::Mode as SelectorMode;
use jsonb::jsonpath::Selector;
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
    let mut paths = Vec::with_capacity(virtual_columns.len());
    for (i, f) in table_schema.fields().iter().enumerate() {
        if f.data_type().remove_nullable() != TableDataType::Variant {
            continue;
        }
        let mut is_src_column = false;
        for virtual_column in &virtual_columns {
            if virtual_column.starts_with(&f.name().clone()) {
                is_src_column = true;
                let name = virtual_column.clone();
                let mut src_name = virtual_column.clone();
                let path = src_name.split_off(f.name().len());
                paths.push((name, src_name, path));
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
    let source_schema = table_schema.project(&field_indices);

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
                operator,
                &write_settings,
                &virtual_loc,
                &source_schema,
                &paths,
                block,
            )
            .await?;
        }
    }

    Ok(())
}

#[async_backtrace::framed]
async fn materialize_virtual_columns(
    operator: &Operator,
    write_settings: &WriteSettings,
    location: &str,
    source_schema: &TableSchema,
    paths: &Vec<(String, String, String)>,
    block: DataBlock,
) -> Result<()> {
    let len = block.num_rows();
    let mut virtual_fields = Vec::with_capacity(paths.len());
    let mut virtual_columns = Vec::with_capacity(paths.len());
    for (virtual_name, src_name, path) in paths {
        let index = source_schema.index_of(src_name).unwrap();
        let virtual_field = TableField::new(
            virtual_name.as_str(),
            TableDataType::Nullable(Box::new(TableDataType::Variant)),
        );
        virtual_fields.push(virtual_field);

        let block_entry = block.get_by_offset(index);
        let column = block_entry
            .value
            .convert_to_full_column(&block_entry.data_type, len);

        let json_path = parse_json_path(path.as_bytes()).unwrap();
        let selector = Selector::new(json_path, SelectorMode::First);

        let mut validity = MutableBitmap::with_capacity(len);
        let mut builder = StringColumnBuilder::with_capacity(len, len * 10);
        for row in 0..len {
            let val = unsafe { column.index_unchecked(row) };
            if let ScalarRef::Variant(v) = val {
                selector.select(v, &mut builder.data, &mut builder.offsets);
                if builder.offsets.len() == row + 2 {
                    validity.push(true);
                    continue;
                }
            }
            validity.push(false);
            builder.commit_row();
        }
        let column = Column::Nullable(Box::new(
            NullableColumn::<VariantType> {
                column: builder.build(),
                validity: validity.into(),
            }
            .upcast(),
        ));
        let virtual_column = BlockEntry::new(
            DataType::Nullable(Box::new(DataType::Variant)),
            Value::Column(column),
        );
        virtual_columns.push(virtual_column);
    }
    let virtual_schema = TableSchemaRefExt::create(virtual_fields);
    let virtual_block = DataBlock::new(virtual_columns, len);

    let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
    let _ = serialize_block(write_settings, &virtual_schema, virtual_block, &mut buffer)?;

    write_data(buffer, operator, location).await?;

    Ok(())
}
