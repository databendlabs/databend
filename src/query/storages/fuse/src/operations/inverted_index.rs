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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::Statistics;
use opendal::Operator;

use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::InvertedIndexWriter;
use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::io::read::read_segment_stats;
use crate::io::write_data;
use crate::operations::BlockMetaIndex;
use crate::operations::CommitSink;
use crate::operations::MutationGenerator;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;
use crate::operations::TableMutationAggregator;

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
        index_name: String,
        index_version: String,
        index_options: &BTreeMap<String, String>,
        index_schema: TableSchemaRef,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<u64> {
        let Some(snapshot) = self.read_table_snapshot().await? else {
            return Ok(0);
        };

        let table_schema = self.schema();
        // Collect field indices used by inverted index.
        let mut field_indices = Vec::new();
        for field in &index_schema.fields {
            let field_index = table_schema.index_of(field.name())?;
            field_indices.push(field_index);
        }

        // Read data here to keep the order of blocks in segment.
        let projection = Projection::Columns(field_indices);

        let block_reader = self.create_block_reader(ctx.clone(), projection, false)?;

        let segment_reader = MetaReaders::segment_info_reader(self.get_operator(), table_schema);

        let target_segments = segment_locs.map(|locations| {
            locations
                .into_iter()
                .filter(|location| snapshot.segments.contains(location))
                .collect::<std::collections::HashSet<_>>()
        });
        if snapshot.segments.is_empty() {
            return Ok(0);
        }

        let operator = self.get_operator_ref();

        // Rebuild only when the block has no explicit metadata, no location, or a different generation.
        let mut block_metas = VecDeque::new();
        for (segment_idx, (segment_loc, ver)) in snapshot.segments.iter().enumerate() {
            if target_segments
                .as_ref()
                .is_some_and(|segments| !segments.contains(&(segment_loc.clone(), *ver)))
            {
                continue;
            }
            let segment_info = segment_reader
                .read(&LoadParams {
                    location: segment_loc.to_string(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: false,
                })
                .await?;
            let stats = match segment_info.summary.additional_stats_loc() {
                Some(location) => Some(read_segment_stats(operator.clone(), location).await?),
                None => None,
            };

            for (block_idx, block_meta) in segment_info.block_metas()?.into_iter().enumerate() {
                let generated = block_meta
                    .inverted_index_meta(&index_name)
                    .is_some_and(|meta| {
                        !meta.location.0.is_empty()
                            && meta.index_version == index_version
                            && meta.location.1 == INVERTED_INDEX_FILE_FORMAT_VERSION
                    });
                if !generated {
                    block_metas.push_back(RefreshInvertedIndexMeta {
                        index: BlockMetaIndex {
                            segment_idx,
                            block_idx,
                        },
                        column_hlls: stats
                            .as_ref()
                            .and_then(|stats| stats.block_hlls.get(block_idx))
                            .cloned(),
                        block_meta,
                    });
                }
            }
        }
        if block_metas.is_empty() {
            return Ok(0);
        }

        let data_schema = Arc::new(DataSchema::from(index_schema.as_ref()));
        let settings = ReadSettings::from_ctx(&ctx)?;
        let write_settings = self.get_write_settings();
        let storage_format = write_settings.storage_format;

        pipeline.add_source(
            |output| {
                let inner = InvertedIndexSource::new(
                    settings,
                    storage_format,
                    block_reader.clone(),
                    block_metas.clone(),
                );
                AsyncSourcer::create(ctx.get_scan_progress(), output, inner)
            },
            1,
        )?;

        let block_nums = block_metas.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(block_nums, max_threads);
        pipeline.try_resize(max_threads)?;
        let meta_location_generator = self.meta_location_generator.clone();
        pipeline.add_async_transformer(|| {
            InvertedIndexTransform::new(
                index_name.clone(),
                index_version.clone(),
                index_options.clone(),
                data_schema.clone(),
                index_schema.clone(),
                operator.clone(),
                meta_location_generator.clone(),
            )
        });

        pipeline.try_resize(1)?;
        let table_meta_timestamps = ctx.get_table_meta_timestamps(self, Some(snapshot.clone()))?;
        pipeline.add_async_accumulating_transformer(|| {
            TableMutationAggregator::create(
                self,
                ctx.clone(),
                snapshot.segments.clone(),
                Default::default(),
                vec![],
                Statistics::default(),
                MutationKind::Refresh,
                table_meta_timestamps,
            )
        });

        let prev_snapshot_id = snapshot.snapshot_id;
        let snapshot_gen = MutationGenerator::new(Some(snapshot), MutationKind::Refresh);
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                None,
                Default::default(),
                snapshot_gen.clone(),
                input,
                None,
                Some(prev_snapshot_id),
                None,
                table_meta_timestamps,
                false,
            )
        })?;

        Ok(block_nums as u64)
    }
}

/// Metadata carried between the refresh source and transform.
#[derive(Clone)]
struct RefreshInvertedIndexMeta {
    index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
    column_hlls: Option<RawBlockHLL>,
}

impl Debug for RefreshInvertedIndexMeta {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.debug_struct("RefreshInvertedIndexMeta").finish()
    }
}

local_block_meta_serde!(RefreshInvertedIndexMeta);

#[typetag::serde(name = "refresh_inverted_index")]
impl BlockMetaInfo for RefreshInvertedIndexMeta {}

/// `InvertedIndexSource` is used to read data blocks that need generate inverted indexes.
pub struct InvertedIndexSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    block_metas: VecDeque<RefreshInvertedIndexMeta>,
    is_finished: bool,
}

impl InvertedIndexSource {
    fn new(
        settings: ReadSettings,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        block_metas: VecDeque<RefreshInvertedIndexMeta>,
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

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        match self.block_metas.pop_front() {
            Some(refresh_meta) => {
                let block = self
                    .block_reader
                    .read_by_meta(
                        &self.settings,
                        &refresh_meta.block_meta,
                        &self.storage_format,
                    )
                    .await?;
                let block = block.add_meta(Some(Box::new(refresh_meta)))?;
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
    data_schema: DataSchemaRef,
    source_schema: TableSchemaRef,
    operator: Operator,
    meta_location_generator: TableMetaLocationGenerator,
}

impl InvertedIndexTransform {
    pub fn new(
        index_name: String,
        index_version: String,
        index_options: BTreeMap<String, String>,
        data_schema: DataSchemaRef,
        source_schema: TableSchemaRef,
        operator: Operator,
        meta_location_generator: TableMetaLocationGenerator,
    ) -> Self {
        Self {
            index_name,
            index_version,
            index_options,
            data_schema,
            source_schema,
            operator,
            meta_location_generator,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for InvertedIndexTransform {
    const NAME: &'static str = "InvertedIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let refresh_meta = data_block
            .get_meta()
            .and_then(RefreshInvertedIndexMeta::downcast_ref_from)
            .unwrap();
        let block_meta = &refresh_meta.block_meta;

        let index_location = self
            .meta_location_generator
            .gen_inverted_index_v2_location(&self.index_version);

        let generate_start = Instant::now();
        let mut writer =
            InvertedIndexWriter::try_create(self.data_schema.clone(), &self.index_options)?;
        writer.add_block(&self.source_schema, &data_block)?;

        let data = writer.finalize()?;
        metrics_inc_block_inverted_index_generate_milliseconds(
            generate_start.elapsed().as_millis() as u64,
        );
        let index_size = data.len() as u64;
        let write_start = Instant::now();
        write_data(data, &self.operator, &index_location).await?;

        metrics_inc_block_inverted_index_write_nums(1);
        metrics_inc_block_inverted_index_write_bytes(index_size);
        metrics_inc_block_inverted_index_write_milliseconds(
            write_start.elapsed().as_millis() as u64
        );

        let mut new_block_meta = Arc::unwrap_or_clone(block_meta.clone());
        let mut index_metas = new_block_meta
            .inverted_index_metas
            .take()
            .unwrap_or_default();
        let new_meta = BlockIndexMeta {
            index_name: self.index_name.clone(),
            location: (index_location, INVERTED_INDEX_FILE_FORMAT_VERSION),
            size: index_size,
            index_version: self.index_version.clone(),
        };
        match index_metas.binary_search_by(|meta| meta.index_name.as_str().cmp(&self.index_name)) {
            Ok(index) => index_metas[index] = new_meta,
            Err(index) => index_metas.insert(index, new_meta),
        }
        new_block_meta.inverted_index_size = Some(index_metas.iter().map(|meta| meta.size).sum());
        new_block_meta.inverted_index_metas = Some(index_metas);

        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
            column_hlls: refresh_meta
                .column_hlls
                .clone()
                .map(BlockHLLState::Serialized),
            column_top_n: None,
        };
        let entry = MutationLogEntry::ReplacedBlock {
            index: refresh_meta.index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        Ok(DataBlock::empty_with_meta(Box::new(MutationLogs {
            entries: vec![entry],
            ..Default::default()
        })))
    }
}

