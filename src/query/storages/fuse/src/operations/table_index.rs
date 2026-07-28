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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_pipeline_transforms::AsyncTransform;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Statistics;
use log::info;
use opendal::Operator;
use uuid::Uuid;

use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::index::BloomIndex;
use crate::index::BloomIndexBuilder;
use crate::index::NgramArgs;
use crate::io::BlockReader;
use crate::io::BlockWriter;
use crate::io::BloomIndexState;
use crate::io::MetaReaders;
use crate::io::SpatialIndexBuilder;
use crate::io::TableMetaLocationGenerator;
use crate::io::VectorIndexBuilder;
use crate::io::read::bloom::block_filter_reader::load_index_meta;
use crate::io::read::load_spatial_index_meta;
use crate::io::read::load_vector_index_meta;
use crate::io::read::read_segment_stats;
use crate::operations::BlockMetaIndex;
use crate::operations::CommitSink;
use crate::operations::MutationGenerator;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;
use crate::operations::TableMutationAggregator;

pub async fn do_refresh_table_index(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    index_name: String,
    index_type: TableIndexType,
    index_schema: TableSchemaRef,
    segment_locs: Option<Vec<Location>>,
    pipeline: &mut Pipeline,
) -> Result<u64> {
    if !matches!(
        index_type,
        TableIndexType::Ngram | TableIndexType::Vector | TableIndexType::Spatial
    ) {
        return Err(ErrorCode::RefreshIndexError(format!(
            "Refresh index type {} not support",
            index_type
        )));
    }
    let table_index = fuse_table
        .get_table_info()
        .meta
        .indexes
        .get(&index_name)
        .ok_or_else(|| ErrorCode::RefreshIndexError(format!("Index: {index_name} not found")))?;

    let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
        // no snapshot
        return Ok(0);
    };

    info!("Start refresh {} index {}", index_type, index_name);

    let table_schema = fuse_table.schema();
    let table_meta = &fuse_table.get_table_info().meta;
    let index_arg = build_refresh_index_arg(
        fuse_table,
        &index_name,
        &index_type,
        table_meta,
        &index_schema,
        &table_schema,
    )?;

    let field_indices = match &index_arg {
        // A refresh publishes one new current-format Bloom file. Rebuild every configured Bloom
        // and Ngram filter from the logical row so old V2/V3 encodings are never relabeled V4.
        RefreshIndexArg::Ngram(arg) => arg.source_field_indices.clone(),
        RefreshIndexArg::Vector(_) | RefreshIndexArg::Spatial(_) => index_schema
            .fields
            .iter()
            .map(|field| table_schema.index_of(field.name()))
            .collect::<Result<Vec<_>>>()?,
    };

    // Read data here to keep the order of blocks in segment.
    let projection = Projection::Columns(field_indices);

    let block_reader = fuse_table.create_block_reader(ctx.clone(), projection, false)?;

    let meta_locations = fuse_table.meta_location_generator().clone();
    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    if snapshot.segments.is_empty() {
        return Ok(0);
    }
    let operator = fuse_table.get_operator_ref();

    let target_segments = segment_locs.map(|locs| locs.into_iter().collect::<HashSet<_>>());

    // Read the segment infos and collect the block metas that need to generate the index.
    let mut index_metas = VecDeque::new();
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
            Some(loc) => Some(read_segment_stats(operator.clone(), loc).await?),
            _ => None,
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
    if index_metas.is_empty() {
        info!(
            "Finish refresh {} index {}, all indexes has generated",
            index_type, index_name
        );
        return Ok(0);
    }

    let settings = ReadSettings::from_ctx(&ctx)?;
    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    pipeline.add_source(
        |output| {
            let inner = IndexSource::new(
                settings,
                storage_format,
                block_reader.clone(),
                index_metas.clone(),
            );
            AsyncSourcer::create(ctx.get_scan_progress(), output, inner)
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
                    ngram_index_arg.bloom_index_type,
                    ngram_index_arg.bloom_columns_map.clone(),
                    ngram_index_arg.ngram_args.clone(),
                    meta_locations.clone(),
                )
            });
        }
        RefreshIndexArg::Vector(_) => {
            let mut table_indexes = BTreeMap::new();
            table_indexes.insert(index_name.clone(), table_index.clone());
            pipeline.add_async_transformer(|| {
                VectorIndexTransform::new(
                    operator.clone(),
                    settings,
                    table_indexes.clone(),
                    index_schema.clone(),
                    meta_locations.clone(),
                )
            });
        }
        RefreshIndexArg::Spatial(spatial_index_arg) => {
            let mut table_indexes = BTreeMap::new();
            table_indexes.insert(index_name.clone(), table_index.clone());
            pipeline.add_async_transformer(|| {
                SpatialIndexTransform::new(
                    operator.clone(),
                    settings,
                    table_indexes.clone(),
                    index_schema.clone(),
                    meta_locations.clone(),
                    spatial_index_arg.existing_names_prefix.clone(),
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

    info!("Finish refresh {} index {}", index_type, index_name);

    Ok(block_nums as u64)
}

// build the index arguments used for refresh
fn build_refresh_index_arg(
    fuse_table: &FuseTable,
    index_name: &String,
    index_type: &TableIndexType,
    table_meta: &TableMeta,
    index_schema: &TableSchemaRef,
    table_schema: &TableSchemaRef,
) -> Result<RefreshIndexArg> {
    match index_type {
        TableIndexType::Ngram => {
            let index_ngram_args =
                FuseTable::create_ngram_index_args(&table_meta.indexes, index_schema, false)?;

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

            let source_schema: TableSchemaRef =
                table_schema.remove_virtual_computed_fields().into();
            let source_field_indices = source_schema
                .fields()
                .iter()
                .map(|field| table_schema.index_of(field.name()))
                .collect::<Result<Vec<_>>>()?;

            let ngram_arg = RefreshNgramIndexArg {
                ngram_index_names,
                source_field_indices,
                bloom_index_type: fuse_table.bloom_index_type(),
                bloom_columns_map: fuse_table
                    .bloom_index_cols()
                    .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?,
                ngram_args: FuseTable::create_ngram_index_args(
                    &table_meta.indexes,
                    &source_schema,
                    false,
                )?,
            };
            Ok(RefreshIndexArg::Ngram(ngram_arg))
        }
        TableIndexType::Vector => {
            let index = table_meta.indexes.get(index_name).unwrap();

            let existing_names_prefix = index
                .column_ids
                .iter()
                .map(|id| format!("{id}"))
                .collect::<Vec<_>>();

            let vector_arg = RefreshVectorIndexArg {
                index_name: index_name.clone(),
                index_version: index.version.clone(),
                existing_names_prefix,
            };
            Ok(RefreshIndexArg::Vector(vector_arg))
        }
        TableIndexType::Spatial => {
            let index = table_meta.indexes.get(index_name).unwrap();

            let existing_names_prefix = index
                .column_ids
                .iter()
                .map(|id| format!("{id}"))
                .collect::<Vec<_>>();

            let spatial_arg = RefreshSpatialIndexArg {
                index_name: index_name.clone(),
                index_version: index.version.clone(),
                existing_column_ids: index.column_ids.clone(),
                existing_names_prefix,
            };
            Ok(RefreshIndexArg::Spatial(spatial_arg))
        }
        _ => unreachable!(),
    }
}

// check if the index has generated
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
        RefreshIndexArg::Spatial(spatial_index_arg) => {
            check_spatial_index_generated(
                operator.clone(),
                segment_idx,
                block_idx,
                block_meta,
                stats,
                spatial_index_arg,
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
    if !block_meta.column_groups.is_empty() {
        return Err(ErrorCode::RefreshIndexError(
            "Ngram index is incompatible with column-group layout".to_string(),
        ));
    }
    if let Some((index_path, _)) = &block_meta.bloom_filter_index_location {
        if let Ok(content_length) = operator
            .stat(index_path)
            .await
            .map(|meta| meta.content_length())
        {
            let bloom_index_meta =
                load_index_meta(operator.clone(), index_path, content_length, None).await?;

            if ngram_index_arg.ngram_index_names.iter().all(|name| {
                bloom_index_meta
                    .columns
                    .iter()
                    .any(|(column_name, _)| column_name == name)
            }) {
                return Ok(None);
            }
        }
    }
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
        index_columns: None,
        index_meta: None,
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
    let mut index_meta = None;
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
                // if metadata has index version, it means the index has generated
                if vector_index_arg.index_version == *index_version {
                    return Ok(None);
                }
            }

            // collect index meta generated by other indexes
            let mut metadata = vector_index_meta.metadata.clone();
            metadata.remove(&vector_index_arg.index_name);
            if !metadata.is_empty() {
                index_meta = Some(metadata);
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
        index_meta,
    };
    Ok(Some(vector_index_meta))
}

async fn check_spatial_index_generated(
    operator: Operator,
    segment_idx: usize,
    block_idx: usize,
    block_meta: Arc<BlockMeta>,
    stats: Option<Arc<SegmentStatistics>>,
    spatial_index_arg: &RefreshSpatialIndexArg,
) -> Result<Option<RefreshIndexMeta>> {
    let mut index_columns = None;
    let mut index_meta = None;
    let mut needs_refresh = match block_meta.spatial_stats.as_ref() {
        Some(stats) => spatial_index_arg
            .existing_column_ids
            .iter()
            .any(|column_id| !stats.keys().any(|id| id == column_id)),
        None => true,
    };

    if let Some(spatial_index_location) = &block_meta.spatial_index_location {
        let index_location = &spatial_index_location.0;
        if let Ok(_content_length) = operator
            .stat(index_location)
            .await
            .map(|meta| meta.content_length())
        {
            let spatial_index_meta =
                load_spatial_index_meta(operator.clone(), index_location).await?;

            let current_index_generated = spatial_index_meta
                .metadata
                .get(&spatial_index_arg.index_name)
                .is_some_and(|version| version == &spatial_index_arg.index_version)
                && spatial_index_arg
                    .existing_names_prefix
                    .iter()
                    .all(|column_id| {
                        spatial_index_meta
                            .columns
                            .iter()
                            .any(|(name, _)| name == column_id)
                    });

            if current_index_generated && !needs_refresh {
                return Ok(None);
            }
            needs_refresh = true;

            let mut metadata = spatial_index_meta.metadata.clone();
            metadata.remove(&spatial_index_arg.index_name);
            if !metadata.is_empty() {
                index_meta = Some(metadata);
            }

            let mut spatial_index_columns = Vec::with_capacity(spatial_index_meta.columns.len());
            for column in &spatial_index_meta.columns {
                let name = column.0.to_string();
                if spatial_index_arg.existing_names_prefix.contains(&name) {
                    continue;
                }
                spatial_index_columns.push(column.clone());
            }
            if !spatial_index_columns.is_empty() {
                index_columns = Some(spatial_index_columns);
            }
        } else {
            needs_refresh = true;
        }
    } else {
        needs_refresh = true;
    }

    if !needs_refresh {
        return Ok(None);
    }

    let spatial_index_meta = RefreshIndexMeta {
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
        index_meta,
    };
    Ok(Some(spatial_index_meta))
}

pub struct IndexSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    index_metas: VecDeque<RefreshIndexMeta>,
    is_finished: bool,
}

impl IndexSource {
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
impl AsyncSource for IndexSource {
    const NAME: &'static str = "IndexSource";

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
    bloom_index_type: BloomIndexType,
    bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ngram_args: Vec<NgramArgs>,
    meta_locations: TableMetaLocationGenerator,
}

impl NgramIndexTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        bloom_index_type: BloomIndexType,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
        ngram_args: Vec<NgramArgs>,
        meta_locations: TableMetaLocationGenerator,
    ) -> Self {
        Self {
            ctx,
            operator,
            bloom_index_type,
            bloom_columns_map,
            ngram_args,
            meta_locations,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for NgramIndexTransform {
    const NAME: &'static str = "NgramIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let RefreshIndexMeta {
            index,
            block_meta,
            column_hlls,
            index_columns: _,
            index_meta: _index_meta,
        } = data_block
            .get_meta()
            .and_then(RefreshIndexMeta::downcast_ref_from)
            .unwrap();

        let mut new_block_meta = Arc::unwrap_or_clone(block_meta.clone());

        let mut builder = BloomIndexBuilder::create(
            self.ctx.get_function_context()?,
            self.bloom_index_type,
            self.bloom_columns_map.clone(),
            &self.ngram_args,
        )?;
        builder.add_block(&data_block)?;

        if let Some(bloom_index) = builder.finalize()? {
            let index_location = self
                .meta_locations
                .block_bloom_index_location(&Uuid::now_v7());
            let state = BloomIndexState::from_bloom_index(&bloom_index, index_location)?;

            new_block_meta.bloom_filter_index_location = Some(state.location.clone());
            new_block_meta.bloom_filter_index_size = state.size();
            new_block_meta.ngram_filter_index_size = state.ngram_size();
            BlockWriter::write_down_bloom_index_state(&self.operator, Some(state)).await?;
        } else {
            return Err(ErrorCode::RefreshIndexError(
                "Refresh Ngram index failed".to_string(),
            ));
        }
        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
            column_hlls: column_hlls.clone().map(BlockHLLState::Serialized),
            column_top_n: None,
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
            ..Default::default()
        };
        let new_block = DataBlock::empty_with_meta(Box::new(meta));
        Ok(new_block)
    }
}

pub struct VectorIndexTransform {
    operator: Operator,
    settings: ReadSettings,
    table_indexes: BTreeMap<String, TableIndex>,
    index_schema: TableSchemaRef,
    meta_locations: TableMetaLocationGenerator,
}

impl VectorIndexTransform {
    pub fn new(
        operator: Operator,
        settings: ReadSettings,
        table_indexes: BTreeMap<String, TableIndex>,
        index_schema: TableSchemaRef,
        meta_locations: TableMetaLocationGenerator,
    ) -> Self {
        Self {
            operator,
            settings,
            table_indexes,
            index_schema,
            meta_locations,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for VectorIndexTransform {
    const NAME: &'static str = "VectorIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let RefreshIndexMeta {
            index,
            block_meta,
            column_hlls,
            index_columns,
            index_meta,
        } = data_block
            .get_meta()
            .and_then(RefreshIndexMeta::downcast_ref_from)
            .unwrap();

        let mut new_block_meta = Arc::unwrap_or_clone(block_meta.clone());

        let mut builder =
            VectorIndexBuilder::try_create(&self.table_indexes, self.index_schema.clone(), false)
                .unwrap();
        builder.add_block(&data_block)?;

        let vector_index_location = self.meta_locations.block_vector_index_location();
        let existing_location = &block_meta.vector_index_location;
        let vector_result = builder
            .finalize_with_existing(
                self.operator.clone(),
                &self.settings,
                &vector_index_location,
                existing_location.as_ref(),
                index_columns.clone(),
                index_meta.clone(),
            )
            .await?;
        let Some(state) = vector_result.index_state else {
            return Err(ErrorCode::Internal("Failed to build vector index"));
        };

        new_block_meta.vector_index_size = Some(state.size);
        new_block_meta.vector_index_location = Some(vector_index_location);
        let mut vector_stats = block_meta.vector_stats.clone().unwrap_or_default();
        if let Some(new_vector_stats) = vector_result.vector_stats {
            vector_stats.extend(new_vector_stats);
        }
        new_block_meta.vector_stats = (!vector_stats.is_empty()).then_some(vector_stats);
        BlockWriter::write_down_vector_index_state(&self.operator, Some(state)).await?;

        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
            column_hlls: column_hlls.clone().map(BlockHLLState::Serialized),
            column_top_n: None,
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
            ..Default::default()
        };
        let new_block = DataBlock::empty_with_meta(Box::new(meta));
        Ok(new_block)
    }
}

pub struct SpatialIndexTransform {
    operator: Operator,
    settings: ReadSettings,
    table_indexes: BTreeMap<String, TableIndex>,
    index_schema: TableSchemaRef,
    meta_locations: TableMetaLocationGenerator,
    existing_names_prefix: Vec<String>,
}

impl SpatialIndexTransform {
    pub fn new(
        operator: Operator,
        settings: ReadSettings,
        table_indexes: BTreeMap<String, TableIndex>,
        index_schema: TableSchemaRef,
        meta_locations: TableMetaLocationGenerator,
        existing_names_prefix: Vec<String>,
    ) -> Self {
        Self {
            operator,
            settings,
            table_indexes,
            index_schema,
            meta_locations,
            existing_names_prefix,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for SpatialIndexTransform {
    const NAME: &'static str = "SpatialIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let RefreshIndexMeta {
            index,
            block_meta,
            column_hlls,
            index_columns,
            index_meta,
        } = data_block
            .get_meta()
            .and_then(RefreshIndexMeta::downcast_ref_from)
            .unwrap();

        let mut new_block_meta = Arc::unwrap_or_clone(block_meta.clone());

        let mut builder =
            SpatialIndexBuilder::try_create(&self.table_indexes, self.index_schema.clone(), false)
                .unwrap();
        builder.add_block(&data_block)?;

        let spatial_index_location = self.meta_locations.block_spatial_index_location();
        let existing_location = &block_meta.spatial_index_location;
        let spatial_result = builder
            .finalize_with_existing(
                self.operator.clone(),
                &self.settings,
                &spatial_index_location,
                existing_location.as_ref(),
                index_columns.clone(),
                index_meta.clone(),
            )
            .await?;

        new_block_meta.spatial_index_size = spatial_result.index_state.as_ref().map(|v| v.size);
        new_block_meta.spatial_index_location = spatial_result
            .index_state
            .as_ref()
            .map(|v| v.location.clone());

        let mut spatial_stats = block_meta.spatial_stats.clone().unwrap_or_default();
        spatial_stats.retain(|column_id, _| {
            !self
                .existing_names_prefix
                .iter()
                .any(|prefix| prefix == &column_id.to_string())
        });
        if let Some(new_spatial_stats) = spatial_result.spatial_stats {
            spatial_stats.extend(new_spatial_stats);
        }
        new_block_meta.spatial_stats = (!spatial_stats.is_empty()).then_some(spatial_stats);

        BlockWriter::write_down_spatial_index_state(&self.operator, spatial_result.index_state)
            .await?;

        let extended_block_meta = ExtendedBlockMeta {
            block_meta: new_block_meta,
            draft_virtual_block_meta: None,
            column_hlls: column_hlls.clone().map(BlockHLLState::Serialized),
            column_top_n: None,
        };

        let entry = MutationLogEntry::ReplacedBlock {
            index: index.clone(),
            block_meta: Arc::new(extended_block_meta),
        };
        let meta = MutationLogs {
            entries: vec![entry],
            ..Default::default()
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
    index_meta: Option<BTreeMap<String, String>>,
}

impl Debug for RefreshIndexMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("RefreshIndexMeta").finish()
    }
}

local_block_meta_serde!(RefreshIndexMeta);

#[typetag::serde(name = "refresh_index")]
impl BlockMetaInfo for RefreshIndexMeta {}

enum RefreshIndexArg {
    Ngram(RefreshNgramIndexArg),
    Vector(RefreshVectorIndexArg),
    Spatial(RefreshSpatialIndexArg),
}

struct RefreshNgramIndexArg {
    ngram_index_names: Vec<String>,
    source_field_indices: Vec<FieldIndex>,
    bloom_index_type: BloomIndexType,
    bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ngram_args: Vec<NgramArgs>,
}

struct RefreshVectorIndexArg {
    index_name: String,
    index_version: String,
    existing_names_prefix: Vec<String>,
}

struct RefreshSpatialIndexArg {
    index_name: String,
    index_version: String,
    existing_column_ids: Vec<ColumnId>,
    existing_names_prefix: Vec<String>,
}
