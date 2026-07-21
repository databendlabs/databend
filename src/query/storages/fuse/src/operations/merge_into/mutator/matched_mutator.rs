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

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use ahash::AHashMap;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::build_origin_block_row_num;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberDataType;
use databend_common_metrics::storage::*;
use databend_common_sql::StreamContext;
use databend_common_storage::MutationStatus;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use itertools::Itertools;
use log::info;

use crate::FuseTable;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::BlockSerialization;
use crate::io::BlockWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::WriteSettings;
use crate::operations::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::merge_into::processors::RowIdKind;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::read_block;

pub(crate) struct AggregationContext {
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    block_builder: BlockBuilder,
    block_reader: Arc<BlockReader>,
    stream_ctx: Option<StreamContext>,
}

type UpdateOffset = HashSet<usize>;
type DeleteOffset = HashSet<usize>;

struct MatchedMutationBatch {
    remaining: AtomicUsize,
    started: Instant,
}

impl MatchedMutationBatch {
    fn new(tasks: usize, started: Instant) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicUsize::new(tasks),
            started,
        })
    }

    pub(crate) fn complete(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            metrics_inc_merge_into_apply_milliseconds(self.started.elapsed().as_millis() as u64);
        }
    }
}

enum MatchedBlockMutationKind {
    Apply {
        index: BlockMetaIndex,
        block_meta: Arc<BlockMeta>,
        modified_offsets: HashSet<usize>,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    },
    Log {
        entry: MutationLogEntry,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    },
}

pub(crate) struct MatchedBlockMutationTask {
    context: Arc<AggregationContext>,
    kind: MatchedBlockMutationKind,
    batch: Arc<MatchedMutationBatch>,
}

impl Debug for MatchedBlockMutationTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchedBlockMutationTask").finish()
    }
}

local_block_meta_serde!(MatchedBlockMutationTask);

#[typetag::serde(name = "matched_block_mutation_task")]
impl BlockMetaInfo for MatchedBlockMutationTask {}

pub(crate) enum PreparedMatchedMutation {
    Log {
        entry: MutationLogEntry,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    },
    Rewrite {
        context: Arc<AggregationContext>,
        index: BlockMetaIndex,
        block: DataBlock,
        origin_stats: Option<databend_storages_common_table_meta::meta::ClusterStatistics>,
        origin_num_rows: usize,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    },
}

impl MatchedBlockMutationTask {
    pub(crate) fn try_from(data: DataBlock) -> Result<Self> {
        let meta = data
            .get_owned_meta()
            .ok_or_else(|| ErrorCode::Internal("matched block mutation task has no metadata"))?;
        Self::downcast_from(meta).ok_or_else(|| {
            ErrorCode::Internal("matched block mutation task metadata has unexpected type")
        })
    }

    pub(crate) async fn prepare(
        self,
    ) -> Result<(Option<PreparedMatchedMutation>, BatchCompletion)> {
        let completion = BatchCompletion(Some(self.batch));
        match self.kind {
            MatchedBlockMutationKind::Log {
                entry,
                logical_updated_rows,
                logical_deleted_rows,
            } => Ok((
                Some(PreparedMatchedMutation::Log {
                    entry,
                    logical_updated_rows,
                    logical_deleted_rows,
                }),
                completion,
            )),
            MatchedBlockMutationKind::Apply {
                index,
                block_meta,
                modified_offsets,
                logical_updated_rows,
                logical_deleted_rows,
            } => {
                let mutation = self
                    .context
                    .apply_update_and_deletion_to_data_block(
                        index,
                        &block_meta,
                        modified_offsets,
                        logical_updated_rows,
                        logical_deleted_rows,
                    )
                    .await?;
                Ok((mutation, completion))
            }
        }
    }
}

pub(crate) struct BatchCompletion(Option<Arc<MatchedMutationBatch>>);

impl BatchCompletion {
    pub(crate) fn complete(mut self) {
        if let Some(batch) = self.0.take() {
            batch.complete();
        }
    }
}

impl PreparedMatchedMutation {
    pub(crate) async fn finish(self) -> Result<(MutationLogEntry, u64, u64)> {
        match self {
            Self::Log {
                entry,
                logical_updated_rows,
                logical_deleted_rows,
            } => Ok((entry, logical_updated_rows, logical_deleted_rows)),
            Self::Rewrite {
                context,
                index,
                block,
                origin_stats,
                origin_num_rows,
                logical_updated_rows,
                logical_deleted_rows,
            } => {
                let serialized = context.block_builder.build(block, |block, generator| {
                    let granule_keys = generator.granule_cluster_key_offsets();
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    info!(
                        "[MERGE-INTO] Serializing block with cluster stats: {:?}",
                        cluster_stats
                    );
                    Ok((cluster_stats, block, granule_keys))
                })?;
                let extended_block_meta = match serialized {
                    BlockSerialization::Pending(pending) => {
                        BlockWriter::write_down(&context.block_builder.operator, pending).await?
                    }
                    BlockSerialization::Written(meta) => meta,
                };
                metrics_inc_merge_into_replace_blocks_counter(1);
                metrics_inc_merge_into_replace_blocks_rows_counter(origin_num_rows as u32);
                Ok((
                    MutationLogEntry::ReplacedBlock {
                        index,
                        block_meta: Arc::new(extended_block_meta),
                    },
                    logical_updated_rows,
                    logical_deleted_rows,
                ))
            }
        }
    }

    pub(crate) fn needs_build(&self) -> bool {
        matches!(self, Self::Rewrite { .. })
    }
}

pub struct MatchedAggregatorConfig {
    ctx: Arc<dyn TableContext>,
    segment_reader: CompactSegmentInfoReader,
    aggregation_ctx: Arc<AggregationContext>,
}

impl MatchedAggregatorConfig {
    pub fn try_create(table: &FuseTable, block_builder: BlockBuilder) -> Result<Self> {
        let ctx = block_builder.ctx.clone();
        let target_table_schema =
            Arc::new(table.schema_with_stream().remove_virtual_computed_fields());
        let data_accessor = table.get_operator();
        let write_settings = table.get_write_settings();
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), target_table_schema.clone());
        let projection = Projection::Columns((0..target_table_schema.num_fields()).collect_vec());
        let block_reader = BlockReader::create(
            ctx.clone(),
            data_accessor,
            target_table_schema.clone(),
            projection,
            false,
        )?;
        let stream_ctx = if table.change_tracking_enabled() {
            Some(StreamContext::try_create(
                ctx.get_function_context()?,
                target_table_schema,
                table.get_table_info().ident.seq,
                true,
                false,
            )?)
        } else {
            None
        };
        Ok(Self {
            ctx,
            segment_reader,
            aggregation_ctx: Arc::new(AggregationContext {
                write_settings,
                read_settings,
                block_builder,
                block_reader,
                stream_ctx,
            }),
        })
    }
}

pub struct MatchedAggregator {
    config: MatchedAggregatorConfig,
    segment_locations: AHashMap<SegmentIndex, Location>,
    block_mutation_row_offset: HashMap<u64, (UpdateOffset, DeleteOffset)>,
    target_build_optimization: bool,
    meta_indexes: HashSet<(SegmentIndex, BlockIndex)>,
    logical_updated_rows: u64,
    logical_deleted_rows: u64,
}

impl MatchedAggregator {
    pub fn create(
        config: MatchedAggregatorConfig,
        segment_locations: Vec<(SegmentIndex, Location)>,
        target_build_optimization: bool,
    ) -> Self {
        Self {
            config,
            block_mutation_row_offset: HashMap::new(),
            segment_locations: AHashMap::from_iter(segment_locations),
            target_build_optimization,
            meta_indexes: HashSet::new(),
            logical_updated_rows: 0,
            logical_deleted_rows: 0,
        }
    }

    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, data_block: DataBlock) -> Result<()> {
        // An optimization: If we use target table as build side, the deduplicate will be done
        // in hashtable probe phase. In this case, we don't support delete for now, so we
        // don't add MutationStatus here.
        if data_block.get_meta().is_some() && data_block.is_empty() {
            if let Some(logs) = MutationLogs::downcast_ref_from(data_block.get_meta().unwrap()) {
                self.logical_updated_rows += logs.logical_updated_rows;
                self.logical_deleted_rows += logs.logical_deleted_rows;
                return Ok(());
            }
            if let Some(meta_index) =
                BlockMetaIndex::downcast_ref_from(data_block.get_meta().unwrap())
            {
                if !self
                    .meta_indexes
                    .insert((meta_index.segment_idx, meta_index.block_idx))
                {
                    info!(
                        "[MERGE-INTO] Duplicated block detected: segment_idx={}, block_idx={}",
                        meta_index.segment_idx, meta_index.block_idx
                    );
                }
            }
            return Ok(());
        }
        if data_block.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let row_id_col = data_block.get_by_offset(0);
        debug_assert!(
            row_id_col.data_type().remove_nullable() == DataType::Number(NumberDataType::UInt64)
        );
        let row_ids = row_id_col.to_column();
        let row_id_kind = RowIdKind::downcast_ref_from(data_block.get_meta().unwrap()).unwrap();
        match row_id_kind {
            RowIdKind::Update => {
                for row_id in row_ids.iter() {
                    let (prefix, offset) =
                        split_row_id(row_id.as_number().unwrap().into_u_int64().unwrap());
                    if !self
                        .block_mutation_row_offset
                        .entry(prefix)
                        .or_insert_with(|| (HashSet::new(), HashSet::new()))
                        .0
                        .insert(offset as usize)
                    {
                        return Err(ErrorCode::UnresolvableConflict(
                            "multi rows from source match one and the same row in the target_table multi times",
                        ));
                    }
                }
            }
            RowIdKind::Delete => {
                let mut num_deleted_rows = 0;
                for row_id in row_ids.iter() {
                    let (prefix, offset) =
                        split_row_id(row_id.as_number().unwrap().into_u_int64().unwrap());
                    let value = self.block_mutation_row_offset.get(&prefix);
                    if value.is_none() || !value.unwrap().1.contains(&(offset as usize)) {
                        num_deleted_rows += 1;
                    }
                    // Support idempotent delete.
                    self.block_mutation_row_offset
                        .entry(prefix)
                        .or_insert_with(|| (HashSet::new(), HashSet::new()))
                        .1
                        .insert(offset as usize);
                }
                self.config
                    .ctx
                    .mutation_state()
                    .add_mutation_status(MutationStatus {
                        insert_rows: 0,
                        update_rows: 0,
                        deleted_rows: num_deleted_rows,
                    });
            }
        };
        metrics_inc_merge_into_accumulate_milliseconds(start.elapsed().as_millis() as u64);
        Ok(())
    }

    #[async_backtrace::framed]
    pub(crate) async fn prepare_tasks(&mut self) -> Result<VecDeque<DataBlock>> {
        let start = Instant::now();
        let mut segment_infos = HashMap::<SegmentIndex, SegmentInfo>::new();
        let segment_indexes = if self.target_build_optimization {
            self.meta_indexes
                .iter()
                .map(|(segment_idx, _)| *segment_idx)
                .collect::<Vec<_>>()
        } else {
            self.block_mutation_row_offset
                .keys()
                .map(|prefix| split_prefix(*prefix).0 as usize)
                .collect::<Vec<_>>()
        };

        for segment_idx in segment_indexes {
            if let Entry::Vacant(entry) = segment_infos.entry(segment_idx) {
                let (path, ver) = self.segment_locations.get(&segment_idx).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, segment (idx {}) not found, during applying mutation log",
                        segment_idx
                    ))
                })?;
                let load_param = LoadParams {
                    location: path.clone(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: true,
                };
                let compact_segment_info = self.config.segment_reader.read(&load_param).await?;
                entry.insert(compact_segment_info.try_into()?);
            }
        }

        let mut kinds = Vec::new();
        if self.target_build_optimization {
            kinds.reserve(self.meta_indexes.len().max(1));
            let mut logical_updated_rows = std::mem::take(&mut self.logical_updated_rows);
            let mut logical_deleted_rows = std::mem::take(&mut self.logical_deleted_rows);
            for (segment_idx, reverse_block_idx) in &self.meta_indexes {
                let segment_info = segment_infos.get(segment_idx).unwrap();
                let block_idx = segment_info.blocks.len() - reverse_block_idx - 1;
                info!(
                    "[MERGE-INTO] Target build optimization applying: segment_idx={}, block_idx={}",
                    segment_idx, block_idx
                );
                kinds.push(MatchedBlockMutationKind::Log {
                    entry: MutationLogEntry::DeletedBlock {
                        index: BlockMetaIndex {
                            segment_idx: *segment_idx,
                            block_idx,
                        },
                    },
                    logical_updated_rows: std::mem::take(&mut logical_updated_rows),
                    logical_deleted_rows: std::mem::take(&mut logical_deleted_rows),
                });
            }
            if kinds.is_empty() && (logical_updated_rows > 0 || logical_deleted_rows > 0) {
                kinds.push(MatchedBlockMutationKind::Log {
                    entry: MutationLogEntry::DoNothing,
                    logical_updated_rows,
                    logical_deleted_rows,
                });
            }
        } else {
            kinds.reserve(self.block_mutation_row_offset.len());
            for (prefix, (update_offsets, delete_offsets)) in &self.block_mutation_row_offset {
                let (segment_idx, reverse_block_idx) = split_prefix(*prefix);
                let segment_idx = segment_idx as usize;
                let segment_info = segment_infos.get(&segment_idx).unwrap();
                let block_idx = segment_info.blocks.len() - reverse_block_idx as usize - 1;
                assert!(block_idx < segment_info.blocks.len());
                info!(
                    "[MERGE-INTO] Applying mutation: segment_idx={}, block_idx={}",
                    segment_idx, block_idx
                );

                let modified_offsets: HashSet<usize> =
                    update_offsets.union(delete_offsets).copied().collect();
                if modified_offsets.len() < update_offsets.len() + delete_offsets.len() {
                    return Err(ErrorCode::UnresolvableConflict(
                        "multi rows from source match one and the same row in the target_table multi times",
                    ));
                }

                kinds.push(MatchedBlockMutationKind::Apply {
                    index: BlockMetaIndex {
                        segment_idx,
                        block_idx,
                    },
                    block_meta: segment_info.blocks[block_idx].clone(),
                    modified_offsets,
                    logical_updated_rows: update_offsets.len() as u64,
                    logical_deleted_rows: delete_offsets.len() as u64,
                });
            }
        }

        if kinds.is_empty() {
            metrics_inc_merge_into_apply_milliseconds(start.elapsed().as_millis() as u64);
            return Ok(VecDeque::new());
        }

        let batch = MatchedMutationBatch::new(kinds.len(), start);
        Ok(kinds
            .into_iter()
            .map(|kind| {
                DataBlock::empty_with_meta(Box::new(MatchedBlockMutationTask {
                    context: self.config.aggregation_ctx.clone(),
                    kind,
                    batch: batch.clone(),
                }))
            })
            .collect())
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_update_and_deletion_to_data_block(
        self: &Arc<Self>,
        index: BlockMetaIndex,
        block_meta: &BlockMeta,
        modified_offsets: HashSet<usize>,
        logical_updated_rows: u64,
        logical_deleted_rows: u64,
    ) -> Result<Option<PreparedMatchedMutation>> {
        info!(
            "[MERGE-INTO] Applying update and delete operations to segment_idx={}, block_idx={}",
            index.segment_idx, index.block_idx
        );
        let mut origin_data_block = read_block(
            self.write_settings.storage_format,
            &self.block_reader,
            block_meta,
            &self.read_settings,
        )
        .await?;
        let origin_num_rows = origin_data_block.num_rows();
        if self.stream_ctx.is_some() {
            origin_data_block.add_entry(build_origin_block_row_num(0, origin_num_rows));
        }

        let mut bitmap = MutableBitmap::new();
        for row in 0..origin_num_rows {
            bitmap.push(!modified_offsets.contains(&row));
        }
        let mut res_block = origin_data_block.filter_with_bitmap(&bitmap.into())?;

        if res_block.is_empty() {
            metrics_inc_merge_into_deleted_blocks_counter(1);
            metrics_inc_merge_into_deleted_blocks_rows_counter(origin_num_rows as u32);
            return Ok(Some(PreparedMatchedMutation::Log {
                entry: MutationLogEntry::DeletedBlock { index },
                logical_updated_rows,
                logical_deleted_rows,
            }));
        }

        if let Some(stream_ctx) = &self.stream_ctx {
            let stream_meta = gen_mutation_stream_meta(None, &block_meta.location.0, 0)?;
            res_block = stream_ctx.apply(res_block, &stream_meta)?;
        }

        Ok(Some(PreparedMatchedMutation::Rewrite {
            context: self.clone(),
            index,
            block: res_block,
            origin_stats: block_meta.cluster_stats.clone(),
            origin_num_rows,
            logical_updated_rows,
            logical_deleted_rows,
        }))
    }
}
