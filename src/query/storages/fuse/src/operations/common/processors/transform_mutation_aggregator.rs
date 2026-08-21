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
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::BlockMetaWithHLL;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VirtualDataSchema;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnPath;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use databend_storages_common_table_meta::meta::encoded_path_from_bracket_name;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;
use itertools::Itertools;
use log::debug;
use log::info;
use log::warn;
use opendal::Operator;

use crate::FuseTable;
use crate::io::CachedMetaWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::io::read::read_segment_stats;
use crate::operations::VirtualSchemaMode;
use crate::operations::common::CommitMeta;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::SnapshotChanges;
use crate::operations::common::SnapshotMerged;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::statistics::VirtualColumnAccumulator;
use crate::statistics::cluster_stats_from_col_stats;
use crate::statistics::prepare_cluster_key_exprs;
use crate::statistics::rebuild_virtual_segment_meta;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::same_partition;
use crate::statistics::sort_by_cluster_stats;

pub struct TableMutationAggregator {
    ctx: Arc<dyn TableContext>,
    table_id: u64,

    base_segments: Vec<Location>,
    merged_blocks: Vec<Arc<ExtendedBlockMeta>>,

    mutations: HashMap<SegmentIndex, BlockMutations>,
    extended_mutations: HashMap<SegmentIndex, ExtendedBlockMutations>,
    appended_segments: Vec<Location>,
    virtual_schema: Option<VirtualDataSchema>,
    virtual_schema_mode: VirtualSchemaMode,
    appended_statistics: Statistics,
    removed_segment_indexes: Vec<SegmentIndex>,
    removed_statistics: Statistics,
    hll: BlockHLL,
    top_n: BlockTopN,
    pending_virtual_paths: Vec<VirtualColumnPath>,
    logical_updated_rows: u64,
    logical_deleted_rows: u64,
    write_segment_ctx: WriteSegmentCtx,

    processed_log_entries: usize,
}

// takes in table mutation logs and aggregates them (former mutation_transform)
#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TableMutationAggregator {
    const NAME: &'static str = "MutationAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let mutation_logs = MutationLogs::try_from(data)?;
        self.logical_updated_rows += mutation_logs.logical_updated_rows;
        self.logical_deleted_rows += mutation_logs.logical_deleted_rows;
        self.processed_log_entries += mutation_logs.entries.len();
        for entry in mutation_logs.entries {
            self.accumulate_log_entry(entry)?;
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        info!(
            "{}: finished aggregating mutation logs, entries: {}",
            self.write_segment_ctx.kind, self.processed_log_entries
        );
        self.generate_append_segments().await?;

        let mut new_segment_locs = Vec::new();
        new_segment_locs.extend(self.appended_segments.clone());

        let conflict_resolve_context = match self.write_segment_ctx.kind {
            MutationKind::Insert => ConflictResolveContext::AppendOnly((
                SnapshotMerged {
                    merged_segments: std::mem::take(&mut self.appended_segments),
                    merged_statistics: std::mem::take(&mut self.appended_statistics),
                },
                self.write_segment_ctx.schema.clone(),
            )),
            MutationKind::Recluster => {
                let mut new_segments = std::mem::take(&mut self.appended_segments);
                let new_segments_len = new_segments.len();
                let removed_segments_len = self.removed_segment_indexes.len();
                let replaced_segments_len = new_segments_len.min(removed_segments_len);
                let mut appended_segments = Vec::new();
                let mut replaced_segments = HashMap::with_capacity(replaced_segments_len);
                if new_segments_len > removed_segments_len {
                    // The remain new segments will be appended.
                    let appended = new_segments.split_off(removed_segments_len);
                    appended_segments.extend(appended.into_iter().rev());
                }

                for (i, location) in new_segments.into_iter().enumerate() {
                    // The old segments will be replaced with the news.
                    replaced_segments.insert(self.removed_segment_indexes[i], location);
                }

                ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
                    appended_segments,
                    removed_segment_indexes: self.removed_segment_indexes[replaced_segments_len..]
                        .to_vec(),
                    replaced_segments,
                    removed_statistics: self.removed_statistics.clone(),
                    merged_statistics: std::mem::take(&mut self.appended_statistics),
                })
            }
            _ => self.apply_mutation(&mut new_segment_locs).await?,
        };
        let meta = CommitMeta::new(
            conflict_resolve_context,
            new_segment_locs,
            self.table_id,
            self.logical_updated_rows,
            self.logical_deleted_rows,
            std::mem::take(&mut self.virtual_schema),
            self.virtual_schema_mode,
            std::mem::take(&mut self.hll),
            std::mem::take(&mut self.top_n),
        );
        debug!("mutations {:?}", meta);
        let block_meta: BlockMetaInfoPtr = Box::new(meta);

        Ok(Some(DataBlock::empty_with_meta(block_meta)))
    }
}

impl TableMutationAggregator {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        merged_blocks: Vec<Arc<ExtendedBlockMeta>>,
        removed_segment_indexes: Vec<usize>,
        removed_statistics: Statistics,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        let virtual_schema = table.table_info.meta.virtual_schema.clone();
        let cluster_key_exprs = table
            .resolve_cluster_keys()
            .map(|cluster_keys| {
                parse_cluster_keys(ctx.clone(), Arc::new(table.clone()), cluster_keys)
                    .map(|keys| keys.into_stats_keys())
            })
            .transpose()
            .expect("table cluster keys should be valid")
            .unwrap_or_default();
        let write_segment_ctx = WriteSegmentCtx {
            dal: table.get_operator(),
            location_gen: table.meta_location_generator().clone(),
            thresholds: table.get_block_thresholds(),
            cluster_key_info: table.cluster_key_info(),
            partition_key_count: table.partition_key_count(),
            cluster_key_exprs: Arc::from(cluster_key_exprs),
            schema: table.schema(),
            kind,
            table_meta_timestamps,
        };
        TableMutationAggregator {
            ctx,
            mutations: HashMap::new(),
            extended_mutations: HashMap::new(),
            appended_segments: vec![],
            virtual_schema,
            virtual_schema_mode: VirtualSchemaMode::Merge,
            base_segments,
            merged_blocks,
            appended_statistics: Statistics::default(),
            removed_segment_indexes,
            removed_statistics,
            hll: HashMap::new(),
            top_n: HashMap::new(),
            pending_virtual_paths: Vec::new(),
            logical_updated_rows: 0,
            logical_deleted_rows: 0,
            write_segment_ctx,
            processed_log_entries: 0,
            table_id: table.get_id(),
        }
    }

    fn accumulate_top_n(&mut self, top_n: Option<BlockTopN>) -> Result<()> {
        if let Some(top_n) = top_n
            && !top_n.is_empty()
        {
            merge_column_top_n_mut(&mut self.top_n, top_n)?;
        }
        Ok(())
    }

    pub fn accumulate_log_entry(&mut self, log_entry: MutationLogEntry) -> Result<()> {
        match log_entry {
            MutationLogEntry::ReplacedBlock { index, block_meta } => {
                // UPDATE replacement blocks contain its after-images. MERGE/REPLACE
                // replacement blocks only preserve unmatched rows; their added
                // images arrive as AppendBlock entries.
                if matches!(self.write_segment_ctx.kind, MutationKind::Update) {
                    BlockHLLState::merge_column_hll(&mut self.hll, &block_meta.column_hlls);
                }
                match self.extended_mutations.entry(index.segment_idx) {
                    Entry::Occupied(mut v) => {
                        v.get_mut().push_replaced(index.block_idx, block_meta);
                    }
                    Entry::Vacant(v) => {
                        v.insert(ExtendedBlockMutations::new_replacement(
                            index.block_idx,
                            block_meta,
                        ));
                    }
                }
            }
            MutationLogEntry::AppendBlock {
                block_meta,
                merge_hll,
            } => {
                // MERGE and REPLACE append logical INSERT/UPDATE after-images.
                if merge_hll
                    || matches!(
                        self.write_segment_ctx.kind,
                        MutationKind::MergeInto | MutationKind::Replace
                    )
                {
                    BlockHLLState::merge_column_hll(&mut self.hll, &block_meta.column_hlls);
                }
                self.accumulate_top_n(block_meta.column_top_n.clone())?;
                self.merged_blocks.push(block_meta);
            }
            MutationLogEntry::DeletedBlock { index } => {
                self.extended_mutations
                    .entry(index.segment_idx)
                    .and_modify(|v| v.push_deleted(index.block_idx))
                    .or_insert(ExtendedBlockMutations::new_deletion(index.block_idx));
            }
            MutationLogEntry::DeletedSegment { deleted_segment } => {
                self.removed_segment_indexes.push(deleted_segment.index);
                merge_statistics_mut(
                    &mut self.removed_statistics,
                    &deleted_segment.summary,
                    self.write_segment_ctx.cluster_key_info.as_ref(),
                );
            }
            MutationLogEntry::AppendSegment {
                segment_location,
                format_version,
                summary,
                hll,
                top_n,
            } => {
                merge_statistics_mut(
                    &mut self.appended_statistics,
                    &summary,
                    self.write_segment_ctx.cluster_key_info.as_ref(),
                );
                if matches!(self.write_segment_ctx.kind, MutationKind::Insert) && !hll.is_empty() {
                    merge_column_hll_mut(&mut self.hll, &hll);
                }
                self.accumulate_top_n(Some(top_n))?;

                self.appended_segments
                    .push((segment_location, format_version));
            }
            MutationLogEntry::AppendVirtualSchema {
                virtual_schema,
                mode,
            } => {
                self.virtual_schema = virtual_schema;
                self.virtual_schema_mode = mode;
            }
            MutationLogEntry::CompactExtras { extras } => {
                match self.mutations.entry(extras.segment_index) {
                    Entry::Occupied(mut v) => {
                        for (idx, blocks) in extras.unchanged_blocks {
                            v.get_mut().replaced_blocks.push((idx, blocks));
                        }
                    }
                    Entry::Vacant(v) => {
                        let mut replaced_blocks = Vec::with_capacity(extras.unchanged_blocks.len());
                        for (idx, blocks) in extras.unchanged_blocks {
                            replaced_blocks.push((idx, blocks));
                        }
                        v.insert(BlockMutations {
                            replaced_blocks,
                            deleted_blocks: vec![],
                        });
                    }
                }

                self.removed_segment_indexes
                    .extend(extras.removed_segment_indexes);
                merge_statistics_mut(
                    &mut self.removed_statistics,
                    &extras.removed_segment_summary,
                    self.write_segment_ctx.cluster_key_info.as_ref(),
                );
            }
            MutationLogEntry::DoNothing => (),
        }
        Ok(())
    }

    async fn generate_append_segments(&mut self) -> Result<()> {
        if self.merged_blocks.is_empty() {
            return Ok(());
        }

        let mut merged_blocks = self.accumulate_merged_blocks()?;

        if let Some(cluster_key_info) = self.write_segment_ctx.cluster_key_info.as_ref() {
            let id = cluster_key_info.cluster_key_id();
            // sort ascending.
            merged_blocks.sort_by(|a, b| {
                sort_by_cluster_stats(a.0.cluster_stats.as_ref(), b.0.cluster_stats.as_ref(), id)
            });
        }

        let mut partition_groups: Vec<Vec<BlockMetaWithHLL>> = Vec::new();
        for block in merged_blocks {
            if partition_groups
                .last()
                .and_then(|group| group.last())
                .is_none_or(|previous| {
                    !same_partition(
                        previous.0.partition_stats.as_ref(),
                        block.0.partition_stats.as_ref(),
                        self.write_segment_ctx.partition_key_count,
                    )
                })
            {
                partition_groups.push(Vec::new());
            }
            partition_groups.last_mut().unwrap().push(block);
        }

        let mut tasks = Vec::new();
        for partition_blocks in partition_groups {
            let segments_num = (partition_blocks.len()
                / self.write_segment_ctx.thresholds.block_per_segment)
                .max(1);
            let chunk_size = partition_blocks.len().div_ceil(segments_num);
            for chunk in &partition_blocks.into_iter().chunks(chunk_size) {
                let (new_blocks, new_hlls): (Vec<Arc<BlockMeta>>, Vec<Option<RawBlockHLL>>) =
                    chunk.unzip();
                // Only compaction/reclustering output may be force-marked perfect to keep
                // those operations at a fixed point. REPLACE/MERGE append after-images
                // must retain the physical perfect-block count from reduce_block_metas.
                let force_all_blocks_perfect = matches!(
                    self.write_segment_ctx.kind,
                    MutationKind::Compact | MutationKind::Recluster
                ) && new_blocks.len() > 1;

                let temporary_schema = build_temporary_virtual_schema(
                    self.virtual_schema.clone(),
                    &self.pending_virtual_paths,
                );
                let input_schemas = vec![temporary_schema; new_blocks.len()];
                let ctx = self.write_segment_ctx.clone();
                tasks.push(async move {
                    // SegmentStatistics encoding and Zstd compression are CPU-heavy. Perform them
                    // in the bounded worker pool rather than serially before the first write.
                    let new_hlls = generate_segment_stats(new_hlls)?;
                    ctx.write_segment(
                        new_blocks,
                        new_hlls,
                        force_all_blocks_perfect,
                        input_schemas,
                    )
                    .await
                });
            }
        }

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        self.pending_virtual_paths.clear();
        let worker_count = max_threads.min(tasks.len());
        let new_segments = execute_futures_in_parallel(
            tasks,
            worker_count,
            worker_count,
            "fuse-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        for (location, stats) in new_segments {
            merge_statistics_mut(
                &mut self.appended_statistics,
                &stats,
                self.write_segment_ctx.cluster_key_info.as_ref(),
            );
            self.appended_segments
                .push((location, SegmentInfo::VERSION));
        }

        Ok(())
    }

    async fn apply_mutation(
        &mut self,
        new_segment_locs: &mut Vec<Location>,
    ) -> Result<ConflictResolveContext> {
        let start = Instant::now();
        let mut count = 0;

        self.accumulate_extended_mutations()?;

        let appended_segments = std::mem::take(&mut self.appended_segments);
        let appended_statistics = std::mem::take(&mut self.appended_statistics);

        let mut replaced_segments = HashMap::new();
        let mut merged_statistics = Statistics::default();
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize;
        let segment_indices = self.mutations.keys().cloned().collect::<Vec<_>>();
        for chunk in segment_indices.chunks(chunk_size) {
            let results = self.partial_apply_mutation(chunk.to_vec()).await?;
            for result in results {
                if let Some((location, summary)) = result.new_segment_info {
                    // replace the old segment location with the new one.
                    let new_segment_loc = (location, SegmentInfo::VERSION);
                    new_segment_locs.push(new_segment_loc.clone());
                    merge_statistics_mut(
                        &mut merged_statistics,
                        &summary,
                        self.write_segment_ctx.cluster_key_info.as_ref(),
                    );
                    replaced_segments.insert(result.index, new_segment_loc);
                } else {
                    self.removed_segment_indexes.push(result.index);
                }

                if let Some(origin_summary) = result.origin_summary {
                    merge_statistics_mut(
                        &mut self.removed_statistics,
                        &origin_summary,
                        self.write_segment_ctx.cluster_key_info.as_ref(),
                    );
                }
            }

            // Refresh status
            {
                count += chunk.len();
                let status = format!(
                    "{}: generate new segment files:{}/{}, cost:{:?}",
                    self.write_segment_ctx.kind,
                    count,
                    segment_indices.len(),
                    start.elapsed()
                );
                self.ctx.set_status_info(&status);
            }
        }

        info!("removed_segment_indexes:{:?}", self.removed_segment_indexes);

        if matches!(self.virtual_schema_mode, VirtualSchemaMode::Merge) {
            self.update_virtual_schema_block_number(&merged_statistics);
        }

        merge_statistics_mut(
            &mut merged_statistics,
            &appended_statistics,
            self.write_segment_ctx.cluster_key_info.as_ref(),
        );

        Ok(ConflictResolveContext::ModifiedSegmentExistsInLatest(
            SnapshotChanges {
                appended_segments,
                replaced_segments,
                removed_segment_indexes: std::mem::take(&mut self.removed_segment_indexes),
                merged_statistics,
                removed_statistics: std::mem::take(&mut self.removed_statistics),
            },
        ))
    }

    async fn partial_apply_mutation(
        &mut self,
        segment_indices: Vec<usize>,
    ) -> Result<Vec<SegmentLite>> {
        let mut tasks = Vec::with_capacity(segment_indices.len());
        for index in segment_indices {
            let segment_mutation = self.mutations.remove(&index).unwrap();
            let location = self.base_segments.get(index).cloned();
            let write_segment_ctx = self.write_segment_ctx.clone();
            let virtual_schema = self.virtual_schema.clone();
            let virtual_paths = self.pending_virtual_paths.clone();

            tasks.push(async move {
                let mut force_all_blocks_perfect = false;
                let (new_blocks, new_hlls, origin_summary) = if let Some(loc) = location {
                    // read the old segment
                    let compact_segment_info = SegmentsIO::read_compact_segment(
                        write_segment_ctx.dal.clone(),
                        loc,
                        write_segment_ctx.schema.clone(),
                        false,
                    )
                    .await?;
                    let mut segment_info = SegmentInfo::try_from(compact_segment_info)?;
                    let old_virtual_schema = segment_info.summary.virtual_segment_schema.clone();
                    let stats = match segment_info.summary.additional_stats_loc() {
                        Some(loc) => {
                            Some(read_segment_stats(write_segment_ctx.dal.clone(), loc).await?)
                        }
                        _ => None,
                    };

                    // take away the blocks, they are being mutated
                    let mut block_editor = std::mem::take(&mut segment_info.blocks)
                        .into_iter()
                        .enumerate()
                        .map(|(block_idx, block_meta)| {
                            let hll = stats
                                .as_ref()
                                .and_then(|v| v.block_hlls.get(block_idx))
                                .cloned();
                            (block_idx, (block_meta, hll))
                        })
                        .collect::<BTreeMap<usize, _>>();

                    let replaced_indexes = segment_mutation
                        .replaced_blocks
                        .iter()
                        .map(|(idx, _)| *idx)
                        .collect::<HashSet<_>>();
                    for (idx, new_meta) in segment_mutation.replaced_blocks {
                        block_editor.insert(idx, new_meta);
                    }
                    for idx in segment_mutation.deleted_blocks {
                        block_editor.remove(&idx);
                    }

                    if block_editor.is_empty() {
                        return Ok(SegmentLite {
                            index,
                            new_segment_info: None,
                            origin_summary: Some(segment_info.summary),
                        });
                    }

                    let temporary_schema =
                        build_temporary_virtual_schema(virtual_schema, &virtual_paths);
                    let mut new_blocks = Vec::with_capacity(block_editor.len());
                    let mut new_hlls = Vec::with_capacity(block_editor.len());
                    let mut input_schemas = Vec::with_capacity(block_editor.len());
                    for (idx, (block, hll)) in block_editor {
                        let schema = if replaced_indexes.contains(&idx) {
                            temporary_schema.clone()
                        } else {
                            old_virtual_schema.clone()
                        };
                        new_blocks.push(block);
                        new_hlls.push(hll);
                        input_schemas.push(schema);
                    }
                    let stats = generate_segment_stats(new_hlls)?;
                    let new_segment_info = write_segment_ctx
                        .write_segment(new_blocks, stats, force_all_blocks_perfect, input_schemas)
                        .await?;
                    return Ok(SegmentLite {
                        index,
                        new_segment_info: Some(new_segment_info),
                        origin_summary: Some(segment_info.summary),
                    });
                } else {
                    // Only compact builds replacement segments without corresponding
                    // entries in base_segments. Treating a missing base segment from
                    // any other mutation as compact output could silently corrupt its
                    // segment statistics.
                    if !matches!(write_segment_ctx.kind, MutationKind::Compact) {
                        return Err(ErrorCode::Internal(format!(
                            "{} mutation references missing base segment index {}",
                            write_segment_ctx.kind, index
                        )));
                    }
                    assert!(segment_mutation.deleted_blocks.is_empty());
                    // There are more than 1 blocks, means that the blocks can no longer be compacted.
                    // They can be marked as perfect blocks.
                    force_all_blocks_perfect = segment_mutation.replaced_blocks.len() > 1;
                    let (new_blocks, new_hlls): (Vec<Arc<BlockMeta>>, Vec<Option<RawBlockHLL>>) =
                        segment_mutation
                            .replaced_blocks
                            .into_iter()
                            .sorted_by(|a, b| a.0.cmp(&b.0))
                            .map(|(_, meta)| meta)
                            .unzip();
                    let stats = generate_segment_stats(new_hlls)?;
                    (new_blocks, stats, None)
                };

                let temporary_schema =
                    build_temporary_virtual_schema(virtual_schema, &virtual_paths);
                let input_schemas = vec![temporary_schema; new_blocks.len()];
                let new_segment_info = write_segment_ctx
                    .write_segment(
                        new_blocks,
                        new_hlls,
                        force_all_blocks_perfect,
                        input_schemas,
                    )
                    .await?;

                Ok(SegmentLite {
                    index,
                    new_segment_info: Some(new_segment_info),
                    origin_summary,
                })
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;

        execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-req-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }

    // Assign columnId to the virtual column in the mutation blocks and generate a new virtual schema.
    fn accumulate_extended_mutations(&mut self) -> Result<()> {
        if self.extended_mutations.is_empty() {
            return Ok(());
        }

        let mut virtual_column_accumulator = VirtualColumnAccumulator::try_create(
            &self.write_segment_ctx.schema,
            &self.virtual_schema,
        );

        let extended_mutations = std::mem::take(&mut self.extended_mutations);
        for (segment_idx, extended_block_mutations) in extended_mutations.into_iter() {
            for (block_idx, extended_block_meta) in
                extended_block_mutations.replaced_blocks.into_iter()
            {
                let new_block_meta = if let Some(draft_virtual_block_meta) =
                    &extended_block_meta.draft_virtual_block_meta
                {
                    let mut new_block_meta = extended_block_meta.block_meta.clone();
                    if let Some(ref mut virtual_column_accumulator) = virtual_column_accumulator {
                        let path_statistics = virtual_column_accumulator.add_path_statistics(
                            draft_virtual_block_meta
                                .path_statistics
                                .as_deref()
                                .unwrap_or_default(),
                        );
                        if draft_virtual_block_meta.virtual_columns.is_some()
                            || !path_statistics.is_empty()
                        {
                            let (virtual_column_metas, virtual_column_size, virtual_location) =
                                if let Some(virtual_columns) =
                                    &draft_virtual_block_meta.virtual_columns
                                {
                                    (
                                        virtual_column_accumulator.add_virtual_column_metas(
                                            &virtual_columns.virtual_column_metas,
                                        ),
                                        virtual_columns.virtual_column_size,
                                        virtual_columns.virtual_location.clone(),
                                    )
                                } else {
                                    (HashMap::new(), 0, (String::new(), 0))
                                };
                            new_block_meta.virtual_block_meta = Some(VirtualBlockMeta {
                                virtual_column_metas,
                                virtual_column_size,
                                virtual_location,
                                path_statistics,
                                virtual_columns_complete: true,
                            });
                        }
                    }
                    Arc::new(new_block_meta)
                } else {
                    Arc::new(extended_block_meta.block_meta.clone())
                };

                let column_hlls =
                    BlockHLLState::encode_column_hll(extended_block_meta.column_hlls.clone())?;
                match self.mutations.entry(segment_idx) {
                    Entry::Occupied(mut v) => {
                        v.get_mut()
                            .push_replaced(block_idx, new_block_meta, column_hlls);
                    }
                    Entry::Vacant(v) => {
                        v.insert(BlockMutations::new_replacement(
                            block_idx,
                            new_block_meta,
                            column_hlls,
                        ));
                    }
                }
            }

            for block_idx in extended_block_mutations.deleted_blocks.into_iter() {
                self.mutations
                    .entry(segment_idx)
                    .and_modify(|v| v.push_deleted(block_idx))
                    .or_insert(BlockMutations::new_deletion(block_idx));
            }
        }

        if let Some(ref mut virtual_column_accumulator) = virtual_column_accumulator {
            self.pending_virtual_paths = virtual_column_accumulator.take_segment_paths();
        }
        self.virtual_schema = if let Some(virtual_column_accumulator) = virtual_column_accumulator {
            virtual_column_accumulator.build_virtual_schema()
        } else {
            None
        };

        Ok(())
    }

    // Assign columnId to the virtual column in the merged blocks and generate a new virtual schema.
    fn accumulate_merged_blocks(&mut self) -> Result<Vec<BlockMetaWithHLL>> {
        let mut virtual_column_accumulator = VirtualColumnAccumulator::try_create(
            &self.write_segment_ctx.schema,
            &self.virtual_schema,
        );
        let extended_merged_blocks = std::mem::take(&mut self.merged_blocks);
        let mut new_merged_blocks = Vec::with_capacity(extended_merged_blocks.len());
        for extended_block_meta in extended_merged_blocks {
            let ExtendedBlockMeta {
                mut block_meta,
                draft_virtual_block_meta,
                column_hlls,
                ..
            } = Arc::unwrap_or_clone(extended_block_meta);

            if let Some(draft_virtual_block_meta) = draft_virtual_block_meta
                && let Some(ref mut virtual_column_accumulator) = virtual_column_accumulator
            {
                let path_statistics = virtual_column_accumulator.add_path_statistics(
                    draft_virtual_block_meta
                        .path_statistics
                        .as_deref()
                        .unwrap_or_default(),
                );
                if draft_virtual_block_meta.virtual_columns.is_some() || !path_statistics.is_empty()
                {
                    let (virtual_column_metas, virtual_column_size, virtual_location) =
                        if let Some(virtual_columns) = draft_virtual_block_meta.virtual_columns {
                            (
                                virtual_column_accumulator.add_virtual_column_metas(
                                    &virtual_columns.virtual_column_metas,
                                ),
                                virtual_columns.virtual_column_size,
                                virtual_columns.virtual_location,
                            )
                        } else {
                            (HashMap::new(), 0, (String::new(), 0))
                        };
                    block_meta.virtual_block_meta = Some(VirtualBlockMeta {
                        virtual_column_metas,
                        virtual_column_size,
                        virtual_location,
                        path_statistics,
                        virtual_columns_complete: true,
                    });
                }
            }

            let column_hlls = BlockHLLState::encode_column_hll(column_hlls)?;
            new_merged_blocks.push((Arc::new(block_meta), column_hlls));
        }

        if let Some(ref mut virtual_column_accumulator) = virtual_column_accumulator {
            self.pending_virtual_paths = virtual_column_accumulator.take_segment_paths();
        }
        self.virtual_schema = if let Some(virtual_column_accumulator) = virtual_column_accumulator {
            virtual_column_accumulator.build_virtual_schema_with_block_number()
        } else {
            None
        };

        Ok(new_merged_blocks)
    }

    fn update_virtual_schema_block_number(&mut self, merged_statistics: &Statistics) {
        if let Some(ref mut virtual_schema) = self.virtual_schema {
            virtual_schema.number_of_blocks +=
                merged_statistics.virtual_block_count.unwrap_or_default();
            let removed_virtual_block_count = self
                .removed_statistics
                .virtual_block_count
                .unwrap_or_default();
            if virtual_schema.number_of_blocks >= removed_virtual_block_count {
                virtual_schema.number_of_blocks -= removed_virtual_block_count;
            } else {
                virtual_schema.number_of_blocks = 0;
            }
        }
    }
}

#[derive(Default)]
struct ExtendedBlockMutations {
    replaced_blocks: Vec<(BlockIndex, Arc<ExtendedBlockMeta>)>,
    deleted_blocks: Vec<BlockIndex>,
}

impl ExtendedBlockMutations {
    fn new_replacement(block_idx: BlockIndex, block_meta: Arc<ExtendedBlockMeta>) -> Self {
        ExtendedBlockMutations {
            replaced_blocks: vec![(block_idx, block_meta)],
            deleted_blocks: vec![],
        }
    }

    fn new_deletion(block_idx: BlockIndex) -> Self {
        ExtendedBlockMutations {
            replaced_blocks: vec![],
            deleted_blocks: vec![block_idx],
        }
    }

    fn push_replaced(&mut self, block_idx: BlockIndex, block_meta: Arc<ExtendedBlockMeta>) {
        self.replaced_blocks.push((block_idx, block_meta));
    }

    fn push_deleted(&mut self, block_idx: BlockIndex) {
        self.deleted_blocks.push(block_idx)
    }
}

#[derive(Default)]
struct BlockMutations {
    replaced_blocks: Vec<(BlockIndex, BlockMetaWithHLL)>,
    deleted_blocks: Vec<BlockIndex>,
}

impl BlockMutations {
    fn new_replacement(
        block_idx: BlockIndex,
        block_meta: Arc<BlockMeta>,
        column_hlls: Option<RawBlockHLL>,
    ) -> Self {
        BlockMutations {
            replaced_blocks: vec![(block_idx, (block_meta, column_hlls))],
            deleted_blocks: vec![],
        }
    }

    fn new_deletion(block_idx: BlockIndex) -> Self {
        BlockMutations {
            replaced_blocks: vec![],
            deleted_blocks: vec![block_idx],
        }
    }

    fn push_replaced(
        &mut self,
        block_idx: BlockIndex,
        block_meta: Arc<BlockMeta>,
        column_hlls: Option<RawBlockHLL>,
    ) {
        self.replaced_blocks
            .push((block_idx, (block_meta, column_hlls)));
    }

    fn push_deleted(&mut self, block_idx: BlockIndex) {
        self.deleted_blocks.push(block_idx)
    }
}

struct SegmentLite {
    // segment index.
    index: usize,
    // new segment location and summary.
    new_segment_info: Option<(String, Statistics)>,
    // origin segment summary.
    origin_summary: Option<Statistics>,
}

#[derive(Clone)]
struct WriteSegmentCtx {
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    thresholds: BlockThresholds,
    cluster_key_info: Option<ClusterKeyInfo>,
    partition_key_count: usize,
    cluster_key_exprs: Arc<[Expr<usize>]>,
    schema: TableSchemaRef,
    kind: MutationKind,
    table_meta_timestamps: TableMetaTimestamps,
}

impl WriteSegmentCtx {
    async fn write_segment(
        &self,
        mut blocks: Vec<Arc<BlockMeta>>,
        stats: Option<Vec<u8>>,
        force_all_blocks_perfect: bool,
        input_schemas: Vec<Option<VirtualSegmentSchema>>,
    ) -> Result<(String, Statistics)> {
        let location = self
            .location_gen
            .gen_segment_info_location(self.table_meta_timestamps, false);
        let virtual_schema = rebuild_virtual_segment_meta(&mut blocks, &input_schemas)?;
        let mut new_summary =
            reduce_block_metas(&blocks, self.thresholds, self.cluster_key_info.as_ref())?;
        if force_all_blocks_perfect {
            // To fix issue #13217.
            if new_summary.block_count > new_summary.perfect_block_count {
                warn!(
                    "{}: generate new segment: {}, perfect_block_count: {}, block_count: {}",
                    self.kind, location, new_summary.perfect_block_count, new_summary.block_count,
                );
                new_summary.perfect_block_count = new_summary.block_count;
            }
        }
        if new_summary.cluster_stats.is_none()
            && let Some(cluster_key_info) = self.cluster_key_info.as_ref()
            && !self.cluster_key_exprs.is_empty()
        {
            let prepared_exprs =
                prepare_cluster_key_exprs(&self.cluster_key_exprs, self.schema.as_ref());
            new_summary.cluster_stats = Some(cluster_stats_from_col_stats(
                &prepared_exprs,
                &new_summary.col_stats,
                cluster_key_info.cluster_key_id(),
                0,
            ));
        }
        if let Some(stats) = stats {
            let segment_stats_location =
                TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                    location.as_str(),
                );
            let additional_stats_meta = AdditionalStatsMeta {
                size: stats.len() as u64,
                location: (segment_stats_location.clone(), SegmentStatistics::VERSION),
                ..Default::default()
            };
            self.dal.write(&segment_stats_location, stats).await?;
            new_summary.additional_stats_meta = Some(additional_stats_meta);
        }

        // create new segment info after all block-local ids have been remapped.
        let mut new_segment = SegmentInfo::new(blocks, new_summary.clone());
        new_segment.summary.virtual_segment_schema = virtual_schema;
        new_segment
            .write_meta_through_cache(&self.dal, &location)
            .await?;
        Ok((location, new_summary))
    }
}

fn build_temporary_virtual_schema(
    legacy_schema: Option<VirtualDataSchema>,
    paths: &[VirtualColumnPath],
) -> Option<VirtualSegmentSchema> {
    let schema = VirtualSegmentSchema::from_pending_paths(
        paths.iter().map(|path| {
            let column = legacy_schema.as_ref().and_then(|schema| {
                schema.fields.iter().find(|field| {
                    field.source_column_id == path.source_column_id
                        && encoded_path_from_bracket_name(&field.name).as_deref()
                            == Some(path.path.as_str())
                })
            });
            (
                path.source_column_id,
                path.path.clone(),
                column.map(|field| (field.column_id, field.data_types.clone())),
            )
        }),
        true,
    );
    (!schema.is_empty()).then_some(schema)
}

fn generate_segment_stats(hlls: Vec<Option<RawBlockHLL>>) -> Result<Option<Vec<u8>>> {
    if hlls.iter().all(|v| v.is_none()) {
        Ok(None)
    } else {
        let blocks = hlls.into_iter().map(|x| x.unwrap_or_default()).collect();
        let data = SegmentStatistics::new(blocks, Vec::new()).to_bytes()?;
        Ok(Some(data))
    }
}
