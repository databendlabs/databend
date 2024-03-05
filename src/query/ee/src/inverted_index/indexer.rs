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

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_storages_fuse::io::InvertedIndexWriter;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::ReadSettings;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::Location;

pub struct Indexer {}

impl Indexer {
    pub(crate) fn new() -> Indexer {
        Indexer {}
    }

    #[async_backtrace::framed]
    pub(crate) async fn index(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        schema: DataSchema,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<String> {
        let snapshot_opt = fuse_table.read_table_snapshot().await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot
            return Ok("".to_string());
        };
        if schema.fields.is_empty() {
            // no field for index
            return Ok("".to_string());
        }

        let table_schema = &fuse_table.get_table_info().meta.schema;

        // Collect field indices used by inverted index.
        let mut field_indices = Vec::new();
        for field in &schema.fields {
            let field_index = table_schema.index_of(field.name())?;
            field_indices.push(field_index);
        }

        let projection = Projection::Columns(field_indices);
        let block_reader =
            fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

        let segment_reader =
            MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

        let settings = ReadSettings::from_ctx(&ctx)?;
        let write_settings = fuse_table.get_write_settings();
        let storage_format = write_settings.storage_format;

        let operator = fuse_table.get_operator_ref();

        // If no segment locations are specified, iterates through all segments
        let segment_locs = if let Some(segment_locs) = segment_locs {
            segment_locs
        } else {
            snapshot.segments.clone()
        };

        let mut index_writer = InvertedIndexWriter::try_create(schema)?;

        for (location, ver) in segment_locs {
            let segment_info = segment_reader
                .read(&LoadParams {
                    location: location.to_string(),
                    len_hint: None,
                    ver,
                    put_cache: false,
                })
                .await?;

            let block_metas = segment_info.block_metas()?;
            for block_meta in block_metas {
                let block = block_reader
                    .read_by_meta(&settings, &block_meta, &storage_format)
                    .await?;

                index_writer.add_block(block)?;
            }
        }

        let location_generator = fuse_table.meta_location_generator();

        let index_location = index_writer.finalize(operator, location_generator).await?;
        // TODO: add index location to meta
        Ok(index_location)
    }
}
