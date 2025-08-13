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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::AsyncTransform;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::index::filters::BlockFilter;
use databend_common_storages_fuse::index::filters::Filter;
use databend_common_storages_fuse::index::BloomIndexBuilder;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_index_meta;
use databend_common_storages_fuse::io::read::load_vector_index_meta;
use databend_common_storages_fuse::io::read::read_segment_stats;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::BlockWriter;
use databend_common_storages_fuse::io::BloomIndexState;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::VectorIndexBuilder;
use databend_common_storages_fuse::operations::BlockMetaIndex;
use databend_common_storages_fuse::operations::CommitSink;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::MutationLogEntry;
use databend_common_storages_fuse::operations::MutationLogs;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_query::storages::index::BloomIndex;
use databend_query::storages::index::NgramArgs;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::FilterImpl;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;

pub async fn do_refresh_table_index(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    index_name: String,
    index_type: TableIndexType,
    index_schema: TableSchemaRef,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
        // no snapshot
        return Ok(());
    };
    let table_schema = &fuse_table.get_table_info().meta.schema;

    // Collect field indices used by index.
    let mut field_indices = Vec::with_capacity(index_schema.fields.len());
    for field in &index_schema.fields {
        let field_index = table_schema.index_of(field.name())?;
        field_indices.push(field_index);
    }

    // Read data here to keep the order of blocks in segment.
    let projection = Projection::Columns(field_indices);

    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let meta_locations = fuse_table.meta_location_generator().clone();
    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    if snapshot.segments.is_empty() {
        return Ok(());
    }
    let operator = fuse_table.get_operator_ref();

    let table_meta = &fuse_table.get_table_info().meta;
    let index_arg = build_refresh_index_arg(
        ctx.clone(),
        &index_name,
        index_type,
        table_meta,
        &index_schema,
        meta_locations,
    )?;

    println!("\n\n\n---------------");

    // Read the segment infos and collect the block metas that need to generate the index.
    let mut index_metas = VecDeque::new();
    for (segment_idx, (segment_loc, ver)) in snapshot.segments.iter().enumerate() {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: segment_loc.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        let stats = if let Some(meta) = &segment_info.summary.additional_stats_meta {
            let stats = read_segment_stats(operator.clone(), meta.location.clone()).await?;
            Some(stats)
        } else {
            None
        };

        for (block_idx, block_meta) in segment_info.block_metas()?.into_iter().enumerate() {
            let Some(refresh_index_meta) = check_index_generated(
                operator.clone(),
                segment_idx,
                block_idx,
                block_meta,
                stats.clone(),
                &index_arg,
            )
            .await?
            else {
                continue;
            };

            index_metas.push_back(refresh_index_meta);
        }
    }
    println!("index_metas={:?}", index_metas);
    if index_metas.is_empty() {
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
                index_metas.clone(),
            );
            AsyncSourcer::create(ctx.clone(), output, inner)
        },
        1,
    )?;

    let block_nums = index_metas.len();
    let max_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_threads = std::cmp::min(block_nums, max_threads);
    pipeline.try_resize(max_threads)?;

    let settings = ReadSettings::from_ctx(&ctx)?;
    match index_arg {
        RefreshIndexArg::Ngram(ngram_index_arg) => {
            pipeline.add_async_transformer(|| {
                NgramIndexTransform::new(
                    ctx.clone(),
                    operator.clone(),
                    ngram_index_arg.index_ngram_args.clone(),
                    ngram_index_arg.ngram_index_names.clone(),
                    ngram_index_arg.existing_names_prefix.clone(),
                )
            });
        }
        RefreshIndexArg::Vector(vector_index_arg) => {
            pipeline.add_async_transformer(|| {
                VectorIndexTransform::new(
                    ctx.clone(),
                    operator.clone(),
                    settings,
                    vector_index_arg.index_name.clone(),
                    vector_index_arg.builder.clone(),
                    vector_index_arg.meta_locations.clone(),
                )
            });
        }
    }

    pipeline.try_resize(1)?;
    let table_meta_timestamps =
        ctx.get_table_meta_timestamps(fuse_table, Some(snapshot.clone()))?;
    pipeline.add_async_accumulating_transformer(|| {
        TableMutationAggregator::create(
            fuse_table,
            ctx.clone(),
            snapshot.segments.clone(),
            vec![],
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

fn build_refresh_index_arg(
    ctx: Arc<dyn TableContext>,
    index_name: &String,
    index_type: TableIndexType,
    table_meta: &TableMeta,
    index_schema: &TableSchemaRef,
    meta_locations: TableMetaLocationGenerator,
) -> Result<RefreshIndexArg> {
    match index_type {
        TableIndexType::Ngram => {
            let index_ngram_args = FuseTable::create_ngram_index_args(table_meta, index_schema)?;

            let existing_names_prefix = index_ngram_args
                .iter()
                .map(|arg| format!("Ngram({})", arg.column_id()))
                .collect::<Vec<_>>();

            let ngram_index_names = index_ngram_args
                .iter()
                .map(|arg| {
                    BloomIndex::build_filter_ngram_name(
                        arg.column_id(),
                        arg.gram_size(),
                        arg.bloom_size(),
                    )
                })
                .collect::<Vec<_>>();

            let ngram_arg = RefreshNgramIndexArg {
                index_ngram_args,
                ngram_index_names,
                existing_names_prefix,
            };
            Ok(RefreshIndexArg::Ngram(ngram_arg))
        }
        TableIndexType::Vector => {
            let index = table_meta.indexes.get(index_name).unwrap();

            let mut table_indexes = BTreeMap::new();
            table_indexes.insert(index_name.clone(), index.clone());
            let builder =
                VectorIndexBuilder::try_create(ctx, &table_indexes, index_schema.clone()).unwrap();

            let mut existing_table_indexes = table_meta.indexes.clone();
            existing_table_indexes.remove(index_name);

            let existing_names_prefix = index
                .column_ids
                .iter()
                .map(|id| format!("{id}"))
                .collect::<Vec<_>>();

            let vector_arg = RefreshVectorIndexArg {
                index_name: index_name.clone(),
                index_version: index.version.clone(),
                existing_names_prefix,
                existing_table_indexes,
                builder,
                meta_locations,
            };
            Ok(RefreshIndexArg::Vector(vector_arg))
        }
        _ => todo!(),
    }
}

async fn check_index_generated(
    operator: Operator,
    segment_idx: usize,
    block_idx: usize,
    block_meta: Arc<BlockMeta>,
    stats: Option<Arc<SegmentStatistics>>,
    index_arg: &RefreshIndexArg,
) -> Result<Option<RefreshIndexMeta>> {
    match index_arg {
        RefreshIndexArg::Ngram(ngram_index_arg) => {
            check_ngram_index_generated(
                operator.clone(),
                segment_idx,
                block_idx,
                block_meta,
                stats,
                ngram_index_arg,
            )
            .await
        }
        RefreshIndexArg::Vector(vector_index_arg) => {
            check_vector_index_generated(
                operator.clone(),
                segment_idx,
                block_idx,
                block_meta,
                stats,
                vector_index_arg,
            )
            .await
        }
    }
}

async fn check_ngram_index_generated(
    operator: Operator,
    segment_idx: usize,
    block_idx: usize,
    block_meta: Arc<BlockMeta>,
    stats: Option<Arc<SegmentStatistics>>,
    ngram_index_arg: &RefreshNgramIndexArg,
) -> Result<Option<RefreshIndexMeta>> {
    let index_location = TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(
        &block_meta.location.0,
    );
    // only generate bloom index if it is not exist.
    let index_columns = if let Ok(content_length) = operator
        .stat(&index_location)
        .await
        .map(|meta| meta.content_length())
    {
        let bloom_index_meta =
            load_index_meta(operator.clone(), &index_location, content_length).await?;

        println!(
            "\n----ngram_index_names={:?}",
            ngram_index_arg.ngram_index_names
        );
        println!(
            "\n----bloom_index_meta.columns={:?}",
            bloom_index_meta.columns
        );

        let mut all_generated = true;
        for ngram_index_name in &ngram_index_arg.ngram_index_names {
            if !bloom_index_meta
                .columns
                .iter()
                .any(|(column_name, _)| column_name == ngram_index_name)
            {
                all_generated = false;
                break;
            }
        }
        println!("all_generated={:?}", all_generated);

        if all_generated {
            return Ok(None);
        }

        Some(bloom_index_meta.columns.clone())
    } else {
        None
    };
    let ngram_index_meta = RefreshIndexMeta {
        index: BlockMetaIndex {
            segment_idx,
            block_idx,
        },
        block_meta,
        column_hlls: stats
            .as_ref()
            .and_then(|v| v.block_hlls.get(block_idx))
            .cloned(),
        index_columns,
    };
    Ok(Some(ngram_index_meta))
}

async fn check_vector_index_generated(
    operator: Operator,
    segment_idx: usize,
    block_idx: usize,
    block_meta: Arc<BlockMeta>,
    stats: Option<Arc<SegmentStatistics>>,
    vector_index_arg: &RefreshVectorIndexArg,
) -> Result<Option<RefreshIndexMeta>> {
    // only generate vector index if it is not exist.
    let mut index_columns = None;
    if let Some(vector_index_location) = &block_meta.vector_index_location {
        let index_location = &vector_index_location.0;
        if let Ok(_content_length) = operator
            .stat(index_location)
            .await
            .map(|meta| meta.content_length())
        {
            let vector_index_meta =
                load_vector_index_meta(operator.clone(), index_location).await?;

            if let Some(index_version) =
                vector_index_meta.metadata.get(&vector_index_arg.index_name)
            {
                // index data exist
                if vector_index_arg.index_version == *index_version {
                    return Ok(None);
                }
            }

            // collect index columns generated by other indexes
            let mut vector_index_columns = Vec::with_capacity(vector_index_meta.columns.len());
            for column in &vector_index_meta.columns {
                let name = column.0.to_string();
                if vector_index_arg
                    .existing_names_prefix
                    .iter()
                    .any(|name_prefix| name.starts_with(name_prefix))
                {
                    continue;
                }
                vector_index_columns.push(column.clone());
            }
            if !vector_index_columns.is_empty() {
                index_columns = Some(vector_index_columns)
            }
        }
    }
    let vector_index_meta = RefreshIndexMeta {
        index: BlockMetaIndex {
            segment_idx,
            block_idx,
        },
        block_meta,
        column_hlls: stats
            .as_ref()
            .and_then(|v| v.block_hlls.get(block_idx))
            .cloned(),
        index_columns,
    };
    Ok(Some(vector_index_meta))
}

pub struct NgramIndexSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    index_metas: VecDeque<RefreshIndexMeta>,
    is_finished: bool,
}

impl NgramIndexSource {
    pub fn new(
        settings: ReadSettings,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        index_metas: VecDeque<RefreshIndexMeta>,
    ) -> Self {
        Self {
            settings,
            storage_format,
            block_reader,
            index_metas,
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

        match self.index_metas.pop_front() {
            Some(index_meta) => {
                let block = self
                    .block_reader
                    .read_by_meta(&self.settings, &index_meta.block_meta, &self.storage_format)
                    .await?;
                let block = block.add_meta(Some(Box::new(index_meta)))?;
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
    index_ngram_args: Vec<NgramArgs>,
    ngram_index_names: Vec<String>,
    existing_names_prefix: Vec<String>,
}

impl NgramIndexTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        index_ngram_args: Vec<NgramArgs>,
        ngram_index_names: Vec<String>,
        existing_names_prefix: Vec<String>,
    ) -> Self {
        println!("index_ngram_args={:?}", index_ngram_args);
        println!("ngram_index_names={:?}", ngram_index_names);
        println!("existing_names_prefix={:?}", existing_names_prefix);
        Self {
            ctx,
            operator,
            index_ngram_args,
            ngram_index_names,
            existing_names_prefix,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for NgramIndexTransform {
    const NAME: &'static str = "NgramIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        println!("\n\n-----trans------block={:?}", data_block);
        let RefreshIndexMeta {
            index,
            block_meta,
            column_hlls,
            index_columns,
        } = data_block
            .get_meta()
            .and_then(RefreshIndexMeta::downcast_ref_from)
            .unwrap();

        let index_path = TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(
            &block_meta.location.0,
        );

        let mut new_block_meta = Arc::unwrap_or_clone(block_meta.clone());
        let index_location = (index_path.clone(), BlockFilter::VERSION);

        let mut builder = BloomIndexBuilder::create(
            self.ctx.get_function_context()?,
            BTreeMap::new(),
            &self.index_ngram_args,
        )?;
        builder.add_block(&data_block)?;

        if let Some(new_ngram_index) = builder.finalize()? {
            // println!("\n\n----new_ngram_index={:?}", new_ngram_index);

            let mut old_ngram_names = Vec::new();
            let bloom_index = if let Some(old_index_columns) = index_columns {
                let mut index_columns = Vec::with_capacity(old_index_columns.len());
                for column in old_index_columns {
                    let name = column.0.to_string();
                    if self
                        .existing_names_prefix
                        .iter()
                        .any(|name_prefix| name.starts_with(name_prefix))
                    {
                        old_ngram_names.push(name);
                        continue;
                    }
                    index_columns.push(name);
                }

                let mut block_filter = load_bloom_filter_by_columns(
                    self.operator.clone(),
                    &index_columns,
                    &index_path,
                    block_meta.bloom_filter_index_size,
                )
                .await?;
                let new_ngram_columns = new_ngram_index.serialize_to_data_block()?.take_columns();
                for new_ngram_column in new_ngram_columns {
                    let (new_filter, _) = FilterImpl::from_bytes(
                        unsafe { new_ngram_column.index_unchecked(0) }
                            .as_binary()
                            .unwrap(),
                    )?;
                    block_filter.filters.push(Arc::new(new_filter));
                }

                let mut new_filter_schema = TableSchema::clone(&block_filter.filter_schema);
                for ngram_index_name in &self.ngram_index_names {
                    new_filter_schema
                        .add_columns(&[TableField::new(ngram_index_name, TableDataType::Binary)])?;
                }
                block_filter.filter_schema = Arc::new(new_filter_schema);

                BloomIndex::from_filter_block(
                    self.ctx.get_function_context()?,
                    block_filter.filter_schema,
                    block_filter.filters,
                    index_location.1,
                )?
            } else {
                new_ngram_index
            };
            let state = BloomIndexState::from_bloom_index(&bloom_index, index_location)?;

            new_block_meta.bloom_filter_index_size = state.size();
            new_block_meta.ngram_filter_index_size = state.ngram_size();
            BlockWriter::write_down_bloom_index_state(&self.operator, Some(state)).await?;

            // remove old bloom index meta and filter
            if let Some(cache) = CacheManager::instance().get_bloom_index_meta_cache() {
                cache.evict(&index_path);
            }
            if let Some(cache) = CacheManager::instance().get_bloom_index_filter_cache() {
                for old_ngram_name in old_ngram_names {
                    cache.evict(&format!("{index_path}-{}", old_ngram_name));
                }
            }
        }
        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
            column_hlls: column_hlls.clone(),
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
        };
        let new_block = DataBlock::empty_with_meta(Box::new(meta));
        Ok(new_block)
    }
}

pub struct VectorIndexTransform {
    _ctx: Arc<dyn TableContext>,
    operator: Operator,
    settings: ReadSettings,
    _index_name: String,
    builder: VectorIndexBuilder,
    meta_locations: TableMetaLocationGenerator,
}

impl VectorIndexTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        settings: ReadSettings,
        index_name: String,
        builder: VectorIndexBuilder,
        meta_locations: TableMetaLocationGenerator,
    ) -> Self {
        Self {
            _ctx: ctx,
            operator,
            settings,
            _index_name: index_name,
            builder,
            meta_locations,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for VectorIndexTransform {
    const NAME: &'static str = "VectorIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        println!("\n\n-----trans------block={:?}", data_block);
        let RefreshIndexMeta {
            index,
            block_meta,
            column_hlls,
            index_columns,
        } = data_block
            .get_meta()
            .and_then(RefreshIndexMeta::downcast_ref_from)
            .unwrap();

        let mut new_block_meta = Arc::unwrap_or_clone(block_meta.clone());

        self.builder.add_block(&data_block)?;

        let vector_index_location = self.meta_locations.block_vector_index_location();
        let existing_location = &block_meta.vector_index_location;
        let state = self
            .builder
            .finalize_with_existing(
                self.operator.clone(),
                &self.settings,
                &vector_index_location,
                existing_location.as_ref(),
                index_columns.clone(),
            )
            .await?;

        new_block_meta.vector_index_size = Some(state.size);
        new_block_meta.vector_index_location = Some(vector_index_location);
        BlockWriter::write_down_vector_index_state(&self.operator, Some(state)).await?;

        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
            column_hlls: column_hlls.clone(),
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
        };
        let new_block = DataBlock::empty_with_meta(Box::new(meta));
        Ok(new_block)
    }
}

#[derive(Clone)]
pub struct RefreshIndexMeta {
    index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
    column_hlls: Option<RawBlockHLL>,
    index_columns: Option<Vec<(String, SingleColumnMeta)>>,
}

impl Debug for RefreshIndexMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("RefreshIndexMeta").finish()
    }
}

local_block_meta_serde!(RefreshIndexMeta);

#[typetag::serde(name = "refresh_index")]
impl BlockMetaInfo for RefreshIndexMeta {}

#[allow(clippy::large_enum_variant)]
enum RefreshIndexArg {
    Ngram(RefreshNgramIndexArg),
    Vector(RefreshVectorIndexArg),
}

struct RefreshNgramIndexArg {
    index_ngram_args: Vec<NgramArgs>,
    ngram_index_names: Vec<String>,
    existing_names_prefix: Vec<String>,
}

struct RefreshVectorIndexArg {
    index_name: String,
    index_version: String,
    existing_names_prefix: Vec<String>,
    #[allow(dead_code)]
    existing_table_indexes: BTreeMap<String, TableIndex>,
    builder: VectorIndexBuilder,
    meta_locations: TableMetaLocationGenerator,
}
