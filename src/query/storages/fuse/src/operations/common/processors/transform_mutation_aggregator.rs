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

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::BlockMetaWithHLL;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VirtualDataSchema;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::VirtualBlockMeta;
use databend_storages_common_table_meta::table::ClusterType;
use itertools::Itertools;
use log::debug;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::read::read_segment_stats;
use crate::io::CachedMetaWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::common::CommitMeta;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::SnapshotChanges;
use crate::operations::common::SnapshotMerged;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::sort_by_cluster_stats;
use crate::statistics::VirtualColumnAccumulator;
use crate::FuseTable;

pub struct TableMutationAggregator {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    table_id: u64,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    thresholds: BlockThresholds,

    default_cluster_key_id: Option<u32>,
    base_segments: Vec<Location>,
    merged_blocks: Vec<Arc<ExtendedBlockMeta>>,
    set_hilbert_level: bool,

    mutations: HashMap<SegmentIndex, BlockMutations>,
    extended_mutations: HashMap<SegmentIndex, ExtendedBlockMutations>,
    appended_segments: Vec<Location>,
    virtual_schema: Option<VirtualDataSchema>,
    appended_statistics: Statistics,
    removed_segment_indexes: Vec<SegmentIndex>,
    removed_statistics: Statistics,
    hll: BlockHLL,

    kind: MutationKind,
    start_time: Instant,
    finished_tasks: usize,
    table_meta_timestamps: TableMetaTimestamps,
}

// takes in table mutation logs and aggregates them (former mutation_transform)
#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TableMutationAggregator {
    const NAME: &'static str = "MutationAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let mutation_logs = MutationLogs::try_from(data)?;
        let task_num = mutation_logs.entries.len();
        mutation_logs.entries.into_iter().for_each(|entry| {
            self.accumulate_log_entry(entry);
        });
        self.refresh_status(task_num);
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        self.generate_append_segments().await?;
        let mut new_segment_locs = Vec::new();
        new_segment_locs.extend(self.appended_segments.clone());

        let conflict_resolve_context = match self.kind {
            MutationKind::Insert => ConflictResolveContext::AppendOnly((
                SnapshotMerged {
                    merged_segments: std::mem::take(&mut self.appended_segments),
                    merged_statistics: std::mem::take(&mut self.appended_statistics),
                },
                self.schema.clone(),
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
                    for location in appended.into_iter().rev() {
                        appended_segments.push(location);
                    }
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
            self.virtual_schema.clone(),
            std::mem::take(&mut self.hll),
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
        let set_hilbert_level = table
            .cluster_type()
            .is_some_and(|v| matches!(v, ClusterType::Hilbert))
            && matches!(
                kind,
                MutationKind::Delete
                    | MutationKind::MergeInto
                    | MutationKind::Replace
                    | MutationKind::Recluster
            );

        let virtual_schema = table.table_info.meta.virtual_schema.clone();
        TableMutationAggregator {
            ctx,
            schema: table.schema(),
            dal: table.get_operator(),
            location_gen: table.meta_location_generator().clone(),
            thresholds: table.get_block_thresholds(),
            default_cluster_key_id: table.cluster_key_id(),
            set_hilbert_level,
            mutations: HashMap::new(),
            extended_mutations: HashMap::new(),
            appended_segments: vec![],
            virtual_schema,
            base_segments,
            merged_blocks,
            appended_statistics: Statistics::default(),
            removed_segment_indexes,
            removed_statistics,
            hll: HashMap::new(),
            kind,
            finished_tasks: 0,
            start_time: Instant::now(),
            table_id: table.get_id(),
            table_meta_timestamps,
        }
    }

    pub fn refresh_status(&mut self, task_num: usize) {
        self.finished_tasks += task_num;

        // Refresh status
        {
            let status = format!(
                "{}: run tasks:{}, cost:{:?}",
                self.kind,
                self.finished_tasks,
                self.start_time.elapsed()
            );
            self.ctx.set_status_info(&status);
        }
    }

    pub fn accumulate_log_entry(&mut self, log_entry: MutationLogEntry) {
        match log_entry {
            MutationLogEntry::ReplacedBlock { index, block_meta } => {
                BlockHLLState::merge_column_hll(&mut self.hll, &block_meta.column_hlls);
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
            MutationLogEntry::AppendBlock { block_meta } => {
                BlockHLLState::merge_column_hll(&mut self.hll, &block_meta.column_hlls);
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
                    self.default_cluster_key_id,
                );
            }
            MutationLogEntry::AppendSegment {
                segment_location,
                format_version,
                summary,
                hll,
            } => {
                merge_statistics_mut(
                    &mut self.appended_statistics,
                    &summary,
                    self.default_cluster_key_id,
                );
                merge_column_hll_mut(&mut self.hll, &hll);

                self.appended_segments
                    .push((segment_location, format_version));
            }
            MutationLogEntry::AppendVirtualSchema { virtual_schema } => {
                self.virtual_schema = Some(virtual_schema.clone());
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
                    self.default_cluster_key_id,
                );
            }
            MutationLogEntry::DoNothing => (),
        }
    }

    async fn generate_append_segments(&mut self) -> Result<()> {
        if self.merged_blocks.is_empty() {
            return Ok(());
        }

        let mut merged_blocks = self.accumulate_merged_blocks()?;

        if let Some(id) = self.default_cluster_key_id {
            // sort ascending.
            merged_blocks
                .sort_by(|a, b| sort_by_cluster_stats(&a.0.cluster_stats, &b.0.cluster_stats, id));
        }

        let mut tasks = Vec::new();
        let segments_num = (merged_blocks.len() / self.thresholds.block_per_segment).max(1);
        let chunk_size = merged_blocks.len().div_ceil(segments_num);
        let default_cluster_key = self.default_cluster_key_id;
        let thresholds = self.thresholds;
        let set_hilbert_level = self.set_hilbert_level;
        let kind = self.kind;
        for chunk in &merged_blocks.into_iter().chunks(chunk_size) {
            let (new_blocks, new_hlls): (Vec<Arc<BlockMeta>>, Vec<Option<RawBlockHLL>>) =
                chunk.unzip();
            let new_hlls = if new_hlls.iter().all(|v| v.is_none()) {
                None
            } else {
                let hlls = new_hlls
                    .into_iter()
                    .map(|x| x.unwrap_or_default())
                    .collect::<Vec<_>>();
                Some(SegmentStatistics::new(hlls).to_bytes()?)
            };
            let all_perfect = new_blocks.len() > 1;

            let location_gen = self.location_gen.clone();
            let op = self.dal.clone();
            let table_meta_timestamps = self.table_meta_timestamps;
            tasks.push(async move {
                write_segment(
                    op,
                    location_gen,
                    new_blocks,
                    new_hlls,
                    thresholds,
                    default_cluster_key,
                    all_perfect,
                    kind,
                    set_hilbert_level,
                    table_meta_timestamps,
                )
                .await
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let new_segments = execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        for (location, stats) in new_segments {
            merge_statistics_mut(
                &mut self.appended_statistics,
                &stats,
                self.default_cluster_key_id,
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
                        self.default_cluster_key_id,
                    );
                    replaced_segments.insert(result.index, new_segment_loc);
                } else {
                    self.removed_segment_indexes.push(result.index);
                }

                if let Some(origin_summary) = result.origin_summary {
                    merge_statistics_mut(
                        &mut self.removed_statistics,
                        &origin_summary,
                        self.default_cluster_key_id,
                    );
                }
            }

            // Refresh status
            {
                count += chunk.len();
                let status = format!(
                    "{}: generate new segment files:{}/{}, cost:{:?}",
                    self.kind,
                    count,
                    segment_indices.len(),
                    start.elapsed()
                );
                self.ctx.set_status_info(&status);
            }
        }

        info!("removed_segment_indexes:{:?}", self.removed_segment_indexes);

        self.update_virtual_schema_block_number(&merged_statistics);

        merge_statistics_mut(
            &mut merged_statistics,
            &appended_statistics,
            self.default_cluster_key_id,
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
        let thresholds = self.thresholds;
        let default_cluster_key_id = self.default_cluster_key_id;
        let kind = self.kind;
        let set_hilbert_level = self.set_hilbert_level;
        let mut tasks = Vec::with_capacity(segment_indices.len());
        for index in segment_indices {
            let segment_mutation = self.mutations.remove(&index).unwrap();
            let location = self.base_segments.get(index).cloned();
            let schema = self.schema.clone();
            let op = self.dal.clone();
            let location_gen = self.location_gen.clone();
            let table_meta_timestamps = self.table_meta_timestamps;

            tasks.push(async move {
                let mut all_perfect = false;
                let mut set_level = false;
                let (new_blocks, new_hlls, origin_summary) = if let Some(loc) = location {
                    // read the old segment
                    let compact_segment_info =
                        SegmentsIO::read_compact_segment(op.clone(), loc, schema, false).await?;
                    let mut segment_info = SegmentInfo::try_from(compact_segment_info)?;

                    let stats = match segment_info.summary.additional_stats_loc() {
                        Some(loc) => Some(read_segment_stats(op.clone(), loc).await?),
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

                    // assign back the mutated blocks to segment
                    let (new_blocks, new_hlls) = block_editor.into_values().unzip();
                    set_level = set_hilbert_level
                        && segment_info
                            .summary
                            .cluster_stats
                            .as_ref()
                            .is_some_and(|v| v.cluster_key_id == default_cluster_key_id.unwrap());
                    let stats = generate_segment_stats(new_hlls)?;
                    (new_blocks, stats, Some(segment_info.summary))
                } else {
                    // use by compact.
                    assert!(segment_mutation.deleted_blocks.is_empty());
                    // There are more than 1 blocks, means that the blocks can no longer be compacted.
                    // They can be marked as perfect blocks.
                    all_perfect = segment_mutation.replaced_blocks.len() > 1;
                    let (new_blocks, new_hlls) = segment_mutation
                        .replaced_blocks
                        .into_iter()
                        .sorted_by(|a, b| a.0.cmp(&b.0))
                        .map(|(_, meta)| meta)
                        .unzip();
                    let stats = generate_segment_stats(new_hlls)?;
                    (new_blocks, stats, None)
                };

                let new_segment_info = write_segment(
                    op,
                    location_gen,
                    new_blocks,
                    new_hlls,
                    thresholds,
                    default_cluster_key_id,
                    all_perfect,
                    kind,
                    set_level,
                    table_meta_timestamps,
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
            self.ctx.clone(),
            &self.schema,
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
                        // generate ColumnId for virtual columns.
                        let virtual_column_metas = virtual_column_accumulator
                            .add_virtual_column_metas(
                                &draft_virtual_block_meta.virtual_column_metas,
                            );

                        let virtual_block_meta = VirtualBlockMeta {
                            virtual_column_metas,
                            virtual_column_size: draft_virtual_block_meta.virtual_column_size,
                            virtual_location: draft_virtual_block_meta.virtual_location.clone(),
                        };
                        new_block_meta.virtual_block_meta = Some(virtual_block_meta);
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
            self.ctx.clone(),
            &self.schema,
            &self.virtual_schema,
        );
        let extended_merged_blocks = std::mem::take(&mut self.merged_blocks);
        let mut new_merged_blocks = Vec::with_capacity(extended_merged_blocks.len());
        for extended_block_meta in extended_merged_blocks.into_iter() {
            let new_block_meta = if let Some(draft_virtual_block_meta) =
                &extended_block_meta.draft_virtual_block_meta
            {
                let mut new_block_meta = extended_block_meta.block_meta.clone();

                if let Some(ref mut virtual_column_accumulator) = virtual_column_accumulator {
                    // generate ColumnId for virtual columns.
                    let virtual_column_metas = virtual_column_accumulator
                        .add_virtual_column_metas(&draft_virtual_block_meta.virtual_column_metas);

                    let virtual_block_meta = VirtualBlockMeta {
                        virtual_column_metas,
                        virtual_column_size: draft_virtual_block_meta.virtual_column_size,
                        virtual_location: draft_virtual_block_meta.virtual_location.clone(),
                    };
                    new_block_meta.virtual_block_meta = Some(virtual_block_meta);
                }
                Arc::new(new_block_meta)
            } else {
                Arc::new(extended_block_meta.block_meta.clone())
            };
            let column_hlls =
                BlockHLLState::encode_column_hll(extended_block_meta.column_hlls.clone())?;
            new_merged_blocks.push((new_block_meta, column_hlls));
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

async fn write_segment(
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    blocks: Vec<Arc<BlockMeta>>,
    stats: Option<Vec<u8>>,
    thresholds: BlockThresholds,
    default_cluster_key: Option<u32>,
    all_perfect: bool,
    kind: MutationKind,
    set_hilbert_level: bool,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<(String, Statistics)> {
    let location = location_gen.gen_segment_info_location(table_meta_timestamps, false);
    let mut new_summary = reduce_block_metas(&blocks, thresholds, default_cluster_key);
    if all_perfect {
        // To fix issue #13217.
        if new_summary.block_count > new_summary.perfect_block_count {
            warn!(
                "{}: generate new segment: {}, perfect_block_count: {}, block_count: {}",
                kind, location, new_summary.perfect_block_count, new_summary.block_count,
            );
            new_summary.perfect_block_count = new_summary.block_count;
        }
    }
    if set_hilbert_level {
        debug_assert!(new_summary.cluster_stats.is_none());
        let level = if thresholds.check_perfect_segment(
            new_summary.block_count as usize,
            new_summary.row_count as usize,
            new_summary.uncompressed_byte_size as usize,
            new_summary.compressed_byte_size as usize,
        ) {
            -1
        } else {
            0
        };
        new_summary.cluster_stats = Some(ClusterStatistics {
            cluster_key_id: default_cluster_key.unwrap(),
            min: vec![],
            max: vec![],
            level,
            pages: None,
        });
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
        dal.write(&segment_stats_location, stats).await?;
        new_summary.additional_stats_meta = Some(additional_stats_meta);
    }

    // create new segment info
    let new_segment = SegmentInfo::new(blocks, new_summary.clone());
    new_segment
        .write_meta_through_cache(&dal, &location)
        .await?;
    Ok((location, new_summary))
}

fn generate_segment_stats(hlls: Vec<Option<RawBlockHLL>>) -> Result<Option<Vec<u8>>> {
    if hlls.iter().all(|v| v.is_none()) {
        Ok(None)
    } else {
        let blocks = hlls.into_iter().map(|x| x.unwrap_or_default()).collect();
        let data = SegmentStatistics::new(blocks).to_bytes()?;
        Ok(Some(data))
    }
}
