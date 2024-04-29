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
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_storages_share::save_share_table_info;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::IndexInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use opendal::Operator;

use crate::io::read::load_inverted_index_info;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::InvertedIndexWriter;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::FuseStorageFormat;
use crate::FuseTable;

impl FuseTable {
    // The big picture of refresh inverted index into pipeline:
    //
    //                                    ┌─────────────────────────┐
    //                             ┌────> │ InvertedIndexTransform1 │ ────┐
    //                             │      └─────────────────────────┘     │
    //                             │                  ...                 │
    // ┌─────────────────────┐     │      ┌─────────────────────────┐     │      ┌───────────────────┐
    // │ InvertedIndexSource │ ────┼────> │ InvertedIndexTransformN │ ────┼────> │ InvertedIndexSink │
    // └─────────────────────┘     │      └─────────────────────────┘     │      └───────────────────┘
    //                             │                  ...                 │
    //                             │      ┌─────────────────────────┐     │
    //                             └────> │ InvertedIndexTransformZ │ ────┘
    //                                    └─────────────────────────┘
    //
    #[inline]
    #[async_backtrace::framed]
    pub async fn do_refresh_inverted_index(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        index_name: String,
        index_version: String,
        index_options: &BTreeMap<String, String>,
        index_schema: TableSchemaRef,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let Some(snapshot) = self.read_table_snapshot().await? else {
            return Ok(());
        };

        let table_schema = &self.get_table_info().meta.schema;
        // Collect field indices used by inverted index.
        let mut field_indices = Vec::new();
        for field in &index_schema.fields {
            let field_index = table_schema.index_of(field.name())?;
            field_indices.push(field_index);
        }

        // Read data here to keep the order of blocks in segment.
        let projection = Projection::Columns(field_indices);

        let block_reader =
            self.create_block_reader(ctx.clone(), projection, false, false, false)?;

        let segment_reader =
            MetaReaders::segment_info_reader(self.get_operator(), table_schema.clone());

        let mut indexed_segments = BTreeSet::new();
        if let Some(inverted_indexes) = &snapshot.inverted_indexes {
            let index_info_loc = inverted_indexes.get(&index_name);
            if let Some(old_index_info) =
                load_inverted_index_info(self.get_operator(), index_info_loc).await?
            {
                // Every time the index info changed, a new index version is generated
                // and the index data needs to be regenerated.
                if old_index_info.index_version == index_version {
                    indexed_segments = old_index_info.indexed_segments.clone();
                }
            }
        }

        // If no segment locations are specified, iterates through all segments
        let segment_locs = if let Some(segment_locs) = &segment_locs {
            segment_locs.clone()
        } else {
            snapshot.segments.clone()
        };

        // Filter segments whose indexes already generated
        let segment_locs: Vec<_> = segment_locs
            .into_iter()
            .filter(|l| !indexed_segments.contains(l))
            .collect();

        if segment_locs.is_empty() {
            return Ok(());
        }

        // Read the segment infos and collect the block metas that need to generate the index.
        let mut block_metas = VecDeque::new();
        for (segment_loc, ver) in &segment_locs {
            let segment_info = segment_reader
                .read(&LoadParams {
                    location: segment_loc.to_string(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: false,
                })
                .await?;

            for block_meta in segment_info.block_metas()? {
                block_metas.push_back(block_meta);
            }
        }
        if block_metas.is_empty() {
            return Ok(());
        }

        let data_schema = DataSchema::from(index_schema.as_ref());
        let settings = ReadSettings::from_ctx(&ctx)?;
        let write_settings = self.get_write_settings();
        let storage_format = write_settings.storage_format;
        let operator = self.get_operator_ref();

        pipeline.add_source(
            |output| {
                let inner = InvertedIndexSource::new(
                    settings,
                    storage_format,
                    block_reader.clone(),
                    block_metas.clone(),
                );
                AsyncSourcer::create(ctx.clone(), output, inner)
            },
            1,
        )?;

        let block_nums = block_metas.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(block_nums, max_threads);
        pipeline.try_resize(max_threads)?;
        let _ = pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(InvertedIndexTransform::try_create(
                input,
                output,
                index_name.clone(),
                index_version.clone(),
                index_options.clone(),
                data_schema.clone(),
                operator.clone(),
            )?))
        });

        pipeline.try_resize(1)?;
        pipeline.add_sink(|input| {
            InvertedIndexSink::try_create(
                input,
                ctx.clone(),
                catalog.clone(),
                table.clone(),
                self.clone(),
                snapshot.clone(),
                index_name.clone(),
                index_version.clone(),
                segment_locs.clone(),
                indexed_segments.clone(),
                block_nums,
            )
        })?;

        Ok(())
    }
}

/// `InvertedIndexSource` is used to read data blocks that need generate inverted indexes.
pub struct InvertedIndexSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    block_metas: VecDeque<Arc<BlockMeta>>,
    is_finished: bool,
}

impl InvertedIndexSource {
    pub fn new(
        settings: ReadSettings,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        block_metas: VecDeque<Arc<BlockMeta>>,
    ) -> Self {
        Self {
            settings,
            storage_format,
            block_reader,
            block_metas,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for InvertedIndexSource {
    const NAME: &'static str = "InvertedIndexSource";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        match self.block_metas.pop_front() {
            Some(block_meta) => {
                let block = self
                    .block_reader
                    .read_by_meta(&self.settings, &block_meta, &self.storage_format)
                    .await?;
                let block = block.add_meta(Some(Box::new(Arc::unwrap_or_clone(block_meta))))?;

                Ok(Some(block))
            }
            None => {
                self.is_finished = true;
                Ok(None)
            }
        }
    }
}

/// `InvertedIndexTransform` is used to generate inverted index for each blocks.
pub struct InvertedIndexTransform {
    index_name: String,
    index_version: String,
    index_options: BTreeMap<String, String>,
    data_schema: DataSchema,
    operator: Operator,
}

impl InvertedIndexTransform {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_name: String,
        index_version: String,
        index_options: BTreeMap<String, String>,
        data_schema: DataSchema,
        operator: Operator,
    ) -> Result<Box<dyn Processor>> {
        Ok(AsyncTransformer::create(input, output, Self {
            index_name,
            index_version,
            index_options,
            data_schema,
            operator,
        }))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for InvertedIndexTransform {
    const NAME: &'static str = "InvertedIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let block_meta = data_block
            .get_meta()
            .and_then(BlockMeta::downcast_ref_from)
            .unwrap();

        let mut index_writer =
            InvertedIndexWriter::try_create(self.data_schema.clone(), &self.index_options)?;
        index_writer.add_block(&data_block)?;

        let index_location =
            TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &block_meta.location.0,
                &self.index_name,
                &self.index_version,
            );

        index_writer
            .finalize(&self.operator, index_location)
            .await?;

        let new_block = DataBlock::new(vec![], 0);
        Ok(new_block)
    }
}

/// `InvertedIndexSink` is used to build inverted index.
pub struct InvertedIndexSink {
    ctx: Arc<dyn TableContext>,
    catalog: Arc<dyn Catalog>,
    table: Arc<dyn Table>,
    fuse_table: FuseTable,
    snapshot: Arc<TableSnapshot>,
    index_name: String,
    index_version: String,
    segment_locs: Vec<Location>,
    indexed_segments: BTreeSet<Location>,
    block_nums: AtomicUsize,
    new_snapshot_loc: Option<String>,
}

impl InvertedIndexSink {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        fuse_table: FuseTable,
        snapshot: Arc<TableSnapshot>,
        index_name: String,
        index_version: String,
        segment_locs: Vec<Location>,
        indexed_segments: BTreeSet<Location>,
        block_nums: usize,
    ) -> Result<ProcessorPtr> {
        let sinker = AsyncSinker::create(input, ctx.clone(), InvertedIndexSink {
            ctx,
            catalog,
            table,
            fuse_table,
            snapshot,
            index_name,
            index_version,
            segment_locs,
            indexed_segments,
            block_nums: AtomicUsize::new(block_nums),
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
                    self.ctx.get_tenant().tenant_name(),
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
    async fn consume(&mut self, _data_block: DataBlock) -> Result<bool> {
        let num = self.block_nums.fetch_sub(1, Ordering::SeqCst);
        if num > 1 {
            return Ok(false);
        }

        // All blocks has generated the inverted indexes,
        // generate a new snapshot
        let mut new_snapshot = TableSnapshot::from_previous(
            self.snapshot.as_ref(),
            Some(self.fuse_table.get_table_info().ident.seq),
        );

        let mut indexed_segments = BTreeSet::new();
        // only keep valid segments, remove not existed ones.
        for indexed_segment in &self.indexed_segments {
            if new_snapshot.segments.contains(indexed_segment) {
                indexed_segments.insert(indexed_segment.clone());
            }
        }
        // add new indexed segments
        for segment_loc in &self.segment_locs {
            indexed_segments.insert(segment_loc.clone());
        }

        let new_index_info = IndexInfo::new(self.index_version.clone(), indexed_segments);
        let index_bytes = new_index_info.to_bytes()?;
        let new_index_info_loc = self
            .fuse_table
            .meta_location_generator()
            .gen_inverted_index_info_location();
        write_data(
            index_bytes,
            self.fuse_table.get_operator_ref(),
            &new_index_info_loc,
        )
        .await?;

        let mut inverted_indexes = new_snapshot.inverted_indexes.clone().unwrap_or_default();
        inverted_indexes.insert(
            self.index_name.clone(),
            (new_index_info_loc, IndexInfo::VERSION),
        );
        new_snapshot.inverted_indexes = Some(inverted_indexes);

        // Write new snapshot file
        let new_snapshot_loc = self
            .fuse_table
            .meta_location_generator()
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

        Ok(true)
    }
}
