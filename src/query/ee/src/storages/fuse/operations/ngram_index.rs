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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::AsyncTransform;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::index::filters::BlockFilter;
use databend_common_storages_fuse::io::write_data;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::BloomIndexState;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::BlockMetaIndex;
use databend_common_storages_fuse::operations::CommitSink;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::MutationLogEntry;
use databend_common_storages_fuse::operations::MutationLogs;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_parquet::ParquetFileReader;
use databend_query::storages::index::BloomIndex;
use databend_query::storages::index::NgramArgs;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;
use opendal::Reader;
use parquet::arrow::async_reader::AsyncFileReader;

pub async fn do_refresh_ngram_index(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    index_name: String,
    segment_locs: Option<Vec<Location>>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let index = fuse_table
        .get_table_info()
        .meta
        .indexes
        .get(&index_name)
        .ok_or_else(|| ErrorCode::RefreshIndexError("Ngram index: {index_name} not found"))?;
    let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
        // no snapshot
        return Ok(());
    };

    let table_schema = &fuse_table.get_table_info().meta.schema;
    // Read blocks in segment.
    let projection = Projection::Columns((0..table_schema.fields().len()).collect());

    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    // If no segment locations are specified, iterates through all segments
    let segment_locs = if let Some(segment_locs) = segment_locs {
        segment_locs
            .into_iter()
            .filter(|s| snapshot.segments.contains(s))
            .collect()
    } else {
        snapshot.segments.clone()
    };

    if segment_locs.is_empty() {
        return Ok(());
    }
    let operator = fuse_table.get_operator_ref();
    let ngram_args = FuseTable::create_ngram_index_args(&fuse_table.get_table_info().meta)?;

    // Read the segment infos and collect the block metas that need to generate the index.
    let mut block_metas = VecDeque::new();
    let mut block_meta_index_map = HashMap::new();
    for (segment_idx, (segment_loc, ver)) in segment_locs.into_iter().enumerate() {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: segment_loc.to_string(),
                len_hint: None,
                ver,
                put_cache: false,
            })
            .await?;

        for (block_idx, block_meta) in segment_info.block_metas()?.into_iter().enumerate() {
            let index_location =
                TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(
                    &block_meta.location.0,
                );
            // only generate bloom index if it is not exist.
            if let Ok(content_length) = operator
                .stat(&index_location)
                .await
                .map(|meta| meta.content_length())
            {
                let reader: Reader = operator.reader(&index_location).await?;
                let mut reader = ParquetFileReader::new(reader, content_length);
                let index_meta = AsyncFileReader::get_metadata(&mut reader, None).await?;
                let schema_meta = index_meta.file_metadata().schema_descr();
                let index_column_id = index.column_ids[0];

                let Some(ngram_arg) = ngram_args
                    .iter()
                    .find(|arg| arg.field().column_id() == index_column_id)
                else {
                    continue;
                };
                let ngram_index_name =
                    BloomIndex::build_filter_ngram_name(index_column_id, ngram_arg.gram_size());
                if schema_meta
                    .columns()
                    .iter()
                    .any(|column| column.name() == ngram_index_name)
                {
                    continue;
                }
            }
            block_meta_index_map.insert(block_meta.location.clone(), BlockMetaIndex {
                segment_idx,
                block_idx,
            });
            block_metas.push_back(block_meta);
        }
    }
    if block_metas.is_empty() {
        return Ok(());
    }

    let settings = ReadSettings::from_ctx(&ctx)?;
    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    pipeline.add_source(
        |output| {
            let inner = NgramIndexSource::new(
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
    pipeline.add_async_transformer(|| {
        NgramIndexTransform::new(
            ctx.clone(),
            operator.clone(),
            ngram_args.clone(),
            block_meta_index_map.clone(),
        )
    });

    pipeline.try_resize(1)?;
    let table_meta_timestamps =
        ctx.get_table_meta_timestamps(fuse_table, Some(snapshot.clone()))?;
    pipeline.add_async_accumulating_transformer(|| {
        TableMutationAggregator::create(
            fuse_table,
            ctx.clone(),
            vec![],
            vec![],
            vec![],
            Statistics::default(),
            MutationKind::Update,
            table_meta_timestamps,
        )
    });

    let prev_snapshot_id = snapshot.snapshot_id;
    let snapshot_gen = MutationGenerator::new(Some(snapshot), MutationKind::Update);
    pipeline.add_sink(|input| {
        CommitSink::try_create(
            fuse_table,
            ctx.clone(),
            None,
            vec![],
            snapshot_gen.clone(),
            input,
            None,
            Some(prev_snapshot_id),
            None,
            table_meta_timestamps,
        )
    })?;

    Ok(())
}

pub struct NgramIndexSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    block_metas: VecDeque<Arc<BlockMeta>>,
    is_finished: bool,
}

impl NgramIndexSource {
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
impl AsyncSource for NgramIndexSource {
    const NAME: &'static str = "NgramIndexSource";

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

pub struct NgramIndexTransform {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    ngram_args: Vec<NgramArgs>,
    block_meta_index_map: HashMap<Location, BlockMetaIndex>,
}

impl NgramIndexTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        ngram_args: Vec<NgramArgs>,
        block_meta_index_map: HashMap<Location, BlockMetaIndex>,
    ) -> Self {
        Self {
            ctx,
            operator,
            ngram_args,
            block_meta_index_map,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for NgramIndexTransform {
    const NAME: &'static str = "NgramIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let block_meta = data_block
            .get_meta()
            .and_then(BlockMeta::downcast_ref_from)
            .unwrap();

        let index_location =
            TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(
                &block_meta.location.0,
            );

        let mut new_block_meta = block_meta.clone();
        if let Some(state) = BloomIndexState::from_data_block(
            self.ctx.clone(),
            &data_block,
            (index_location.clone(), BlockFilter::VERSION),
            BTreeMap::new(),
            &self.ngram_args,
        )? {
            new_block_meta.ngram_filter_index_size = state.ngram_size();
            write_data(state.data(), &self.operator, &index_location).await?;
        }
        let block_meta_index = self
            .block_meta_index_map
            .remove(&block_meta.location)
            .unwrap();
        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: block_meta_index,
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
        };
        let new_block = DataBlock::empty_with_meta(Box::new(meta));
        Ok(new_block)
    }
}
