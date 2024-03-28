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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::InvertedIndexMeta;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::OneBlockSource;
use databend_common_storages_share::save_share_table_info;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::IndexInfo;
use databend_storages_common_table_meta::meta::IndexSegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::io::read::load_inverted_index_info;
use crate::io::write_data;
use crate::io::InvertedIndexWriter;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::FuseTable;
use crate::DEFAULT_ROW_PER_INDEX;

impl FuseTable {
    #[inline]
    #[async_backtrace::framed]
    pub async fn do_refresh_inverted_index(
        &self,
        ctx: Arc<dyn TableContext>,
        meta: InvertedIndexMeta,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                let block = DataBlock::empty_with_meta(Box::new(meta.clone()));
                OneBlockSource::create(output, block)
            },
            1,
        )?;

        pipeline.try_resize(1)?;
        pipeline.add_sink(|input| {
            InvertedIndexSink::try_create(
                input,
                ctx.clone(),
                catalog.clone(),
                table.clone(),
                self.clone(),
            )
        })?;

        Ok(())
    }
}

/// `InvertedIndexSink` is used to build inverted index.
pub struct InvertedIndexSink {
    ctx: Arc<dyn TableContext>,
    catalog: Arc<dyn Catalog>,
    table: Arc<dyn Table>,
    fuse_table: FuseTable,
    new_snapshot_loc: Option<String>,
}

impl InvertedIndexSink {
    pub fn try_create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        fuse_table: FuseTable,
    ) -> Result<ProcessorPtr> {
        let sinker = AsyncSinker::create(input, ctx.clone(), InvertedIndexSink {
            ctx,
            catalog,
            table,
            fuse_table,
            new_snapshot_loc: None,
        });
        Ok(ProcessorPtr::create(sinker))
    }
}

#[async_trait]
impl AsyncSink for InvertedIndexSink {
    const NAME: &'static str = "InvertedIndexSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        if let Some(new_snapshot_loc) = &self.new_snapshot_loc {
            // generate new table meta with new snapshot location
            let mut new_table_meta = self.table.get_table_info().meta.clone();

            new_table_meta.options.insert(
                OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
                new_snapshot_loc.clone(),
            );

            let table_info = self.table.get_table_info();
            let table_id = table_info.ident.table_id;
            let table_version = table_info.ident.seq;

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta,
                copied_files: None,
                deduplicated_label: None,
                update_stream_meta: vec![],
            };

            let res = self.catalog.update_table_meta(table_info, req).await?;

            if let Some(share_table_info) = res.share_table_info {
                save_share_table_info(
                    self.ctx.get_tenant().name(),
                    self.ctx.get_data_operator()?.operator(),
                    share_table_info,
                )
                .await?;
            }
        }

        Ok(())
    }

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let inverted_index_meta = data_block
            .get_meta()
            .and_then(InvertedIndexMeta::downcast_ref_from);

        let Some(inverted_index_meta) = inverted_index_meta else {
            return Ok(false);
        };
        let Some(snapshot) = self.fuse_table.read_table_snapshot().await? else {
            return Ok(false);
        };

        let table_schema = &self.fuse_table.get_table_info().meta.schema;

        // Collect field indices used by inverted index.
        let mut field_indices = Vec::new();
        for field in &inverted_index_meta.index_schema.fields {
            let field_index = table_schema.index_of(field.name())?;
            field_indices.push(field_index);
        }

        // Read data here to keep the order of blocks in segment.
        let projection = Projection::Columns(field_indices);
        let block_reader = self.fuse_table.create_block_reader(
            self.ctx.clone(),
            projection,
            false,
            false,
            false,
        )?;

        let segment_reader =
            MetaReaders::segment_info_reader(self.fuse_table.get_operator(), table_schema.clone());

        let index_info_loc = match &snapshot.index_info_locations {
            Some(locations) => locations.get(&inverted_index_meta.index_name),
            None => None,
        };
        let old_index_info =
            load_inverted_index_info(self.fuse_table.get_operator(), index_info_loc).await?;

        let settings = ReadSettings::from_ctx(&self.ctx)?;
        let write_settings = self.fuse_table.get_write_settings();
        let storage_format = write_settings.storage_format;

        let operator = self.fuse_table.get_operator_ref();

        // If no segment locations are specified, iterates through all segments
        let segment_locs = if let Some(segment_locs) = &inverted_index_meta.segment_locs {
            segment_locs.clone()
        } else {
            snapshot.segments.clone()
        };

        let data_schema = DataSchema::from(inverted_index_meta.index_schema.as_ref());

        // Grouping of segments, each group includes a number of segments to generate an index file.
        // Limit the index file size by check the sum row count to avoid too large index file.
        let mut sum_row_count = 0;
        let mut segments = Vec::new();
        let mut grouped_segments = Vec::new();
        for (segment_loc, ver) in segment_locs {
            // If segment has be indexed, we can ignore it.
            if let Some(ref old_index_info) = old_index_info {
                if old_index_info.indexed_segments.contains(&segment_loc) {
                    continue;
                }
            }
            let segment_info = segment_reader
                .read(&LoadParams {
                    location: segment_loc.to_string(),
                    len_hint: None,
                    ver,
                    put_cache: false,
                })
                .await?;
            sum_row_count += segment_info.summary.row_count as usize;
            segments.push((segment_loc, segment_info));
            // Check sum of row count to prevent index getting too large.
            // TODO: suitable `DEFAULT_ROW_PER_INDEX` requires further testing.
            if sum_row_count > DEFAULT_ROW_PER_INDEX {
                let curr_segments = std::mem::take(&mut segments);
                grouped_segments.push(curr_segments);
                sum_row_count = 0;
            }
        }
        if !segments.is_empty() {
            grouped_segments.push(segments);
        }

        // All segments are already indexed and no need to do further operation.
        // TODO: Segments may require further regrouping to optimise the index data.
        if grouped_segments.is_empty() {
            return Ok(false);
        }
        let location_generator = self.fuse_table.meta_location_generator();

        // Generate index data for each group of segments.
        let mut indexes = BTreeMap::<String, Vec<IndexSegmentInfo>>::new();
        let mut indexed_segments = BTreeSet::<String>::new();
        for segments in grouped_segments {
            let mut index_segments = Vec::with_capacity(segments.len());
            let mut index_writer = InvertedIndexWriter::try_create(data_schema.clone())?;
            for (segment_loc, segment_info) in segments {
                let mut row_count = 0;
                let block_metas = segment_info.block_metas()?;
                for block_meta in block_metas {
                    let block = block_reader
                        .read_by_meta(&settings, &block_meta, &storage_format)
                        .await?;

                    index_writer.add_block(block)?;
                    row_count += block_meta.row_count;
                }
                indexed_segments.insert(segment_loc.to_string());
                let index_segment = IndexSegmentInfo {
                    segment_location: segment_loc.to_string(),
                    row_count,
                    block_range: None,
                };
                index_segments.push(index_segment);
            }

            let index_location = index_writer.finalize(operator, location_generator).await?;
            indexes.insert(index_location, index_segments);
        }

        // Merge old index info
        // TODO: remove unused index file
        if let Some(old_index_info) = old_index_info {
            for (index_loc, index_segments) in &old_index_info.indexes {
                indexes.insert(index_loc.to_string(), index_segments.to_vec());
            }
            for segment_loc in &old_index_info.indexed_segments {
                indexed_segments.insert(segment_loc.to_string());
            }
        }

        // Write new index info file
        let index_info = IndexInfo::new(
            inverted_index_meta.index_schema.clone(),
            indexes,
            indexed_segments,
        );
        let index_bytes = index_info.to_bytes()?;
        let new_index_info_loc = location_generator.gen_inverted_index_info_location();

        write_data(index_bytes, operator, &new_index_info_loc).await?;

        // Generate new table snapshot file
        let mut new_snapshot = TableSnapshot::from_previous(
            snapshot.as_ref(),
            Some(self.fuse_table.get_table_info().ident.seq),
        );
        let mut index_info_locations = BTreeMap::new();
        if let Some(old_index_info_locations) = &snapshot.index_info_locations {
            for (old_index_name, location) in old_index_info_locations {
                if *old_index_name == inverted_index_meta.index_name {
                    continue;
                }
                index_info_locations.insert(old_index_name.to_string(), location.clone());
            }
        }
        index_info_locations.insert(
            inverted_index_meta.index_name.to_string(),
            (new_index_info_loc, IndexInfo::VERSION),
        );

        new_snapshot.index_info_locations = Some(index_info_locations);

        // Write new snapshot file
        let new_snapshot_loc = location_generator
            .snapshot_location_from_uuid(&new_snapshot.snapshot_id, TableSnapshot::VERSION)?;

        let data = new_snapshot.to_bytes()?;
        self.fuse_table
            .get_operator_ref()
            .write(&new_snapshot_loc, data)
            .await?;

        // Write new snapshot hint
        FuseTable::write_last_snapshot_hint(
            self.fuse_table.get_operator_ref(),
            self.fuse_table.meta_location_generator(),
            new_snapshot_loc.clone(),
        )
        .await;

        self.new_snapshot_loc = Some(new_snapshot_loc);

        Ok(false)
    }
}
