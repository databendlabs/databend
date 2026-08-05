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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use ahash::AHashMap;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::UInt64Type;
use databend_common_metrics::storage::*;
use databend_common_sql::StreamContext;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::OnConflictField;
use databend_storages_common_cache::BlockMetaCache;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use log::info;
use log::warn;
use opendal::Operator;

use crate::FuseTable;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::BlockSerialization;
use crate::io::BlockWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::WriteSettings;
use crate::io::read::bloom::block_filter_reader::BloomBlockFilterReader;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::mutation::SegmentIndex;
use crate::operations::read_block;
use crate::operations::replace_into::meta::DeletionByColumn;
use crate::operations::replace_into::meta::ReplaceIntoOperation;
use crate::operations::replace_into::meta::UniqueKeyDigest;
use crate::operations::replace_into::mutator::DeletionAccumulator;
use crate::operations::replace_into::mutator::row_hash_of_columns;

pub(crate) struct AggregationContext {
    segment_locations: AHashMap<SegmentIndex, Location>,
    block_slots_in_charge: Option<BlockSlotDescription>,
    // the fields specified in ON CONFLICT clause
    on_conflict_fields: Vec<OnConflictField>,
    // the field indexes of `on_conflict_fields`
    // which we should apply bloom filtering, if any
    bloom_filter_column_indexes: Vec<FieldIndex>,
    // table fields excludes `on_conflict_fields`
    remain_column_field_ids: Vec<FieldIndex>,
    // reader that reads the ON CONFLICT key fields
    key_column_reader: Arc<BlockReader>,
    // reader that reads the `remain_column_field_ids`
    remain_column_reader: Option<Arc<BlockReader>>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    segment_reader: CompactSegmentInfoReader,
    block_builder: BlockBuilder,
    // generate stream columns if necessary
    stream_ctx: Option<StreamContext>,

    block_meta_cache: Option<BlockMetaCache>,
}

struct ReplaceMutationBatch {
    remaining: AtomicUsize,
    started: Instant,
}

impl ReplaceMutationBatch {
    fn new(tasks: usize, started: Instant) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicUsize::new(tasks),
            started,
        })
    }

    pub(crate) fn complete(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            metrics_inc_replace_apply_deletion_time_ms(self.started.elapsed().as_millis() as u64);
        }
    }
}

pub(crate) struct ReplaceBlockMutationTask {
    context: Arc<AggregationContext>,
    index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
    deleted_key_hashes: ahash::HashSet<UniqueKeyDigest>,
    bloom_hashes: Vec<Vec<u64>>,
    batch: Arc<ReplaceMutationBatch>,
}

impl Debug for ReplaceBlockMutationTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaceBlockMutationTask").finish()
    }
}

local_block_meta_serde!(ReplaceBlockMutationTask);

#[typetag::serde(name = "replace_block_mutation_task")]
impl BlockMetaInfo for ReplaceBlockMutationTask {}

pub(crate) enum PreparedReplaceMutation {
    Log {
        entry: MutationLogEntry,
        logical_deleted_rows: u64,
    },
    Rewrite {
        context: Arc<AggregationContext>,
        index: BlockMetaIndex,
        block: DataBlock,
        origin_stats: Option<databend_storages_common_table_meta::meta::ClusterStatistics>,
        original_rows: usize,
        logical_deleted_rows: u64,
    },
}

pub(crate) struct ReplaceBatchCompletion(Option<Arc<ReplaceMutationBatch>>);

impl ReplaceBatchCompletion {
    pub(crate) fn complete(mut self) {
        if let Some(batch) = self.0.take() {
            batch.complete();
        }
    }
}

impl ReplaceBlockMutationTask {
    pub(crate) fn try_from(data: DataBlock) -> Result<Self> {
        let meta = data
            .get_owned_meta()
            .ok_or_else(|| ErrorCode::Internal("replace block mutation task has no metadata"))?;
        Self::downcast_from(meta).ok_or_else(|| {
            ErrorCode::Internal("replace block mutation task metadata has unexpected type")
        })
    }

    pub(crate) async fn prepare(
        self,
    ) -> Result<(Option<PreparedReplaceMutation>, ReplaceBatchCompletion)> {
        let completion = ReplaceBatchCompletion(Some(self.batch));
        let keys = (self.deleted_key_hashes, self.bloom_hashes);
        let mutation = self
            .context
            .apply_deletion_to_data_block(self.index, &self.block_meta, &keys)
            .await?;
        Ok((mutation, completion))
    }
}

impl PreparedReplaceMutation {
    pub(crate) async fn finish(self) -> Result<(MutationLogEntry, u64)> {
        match self {
            Self::Log {
                entry,
                logical_deleted_rows,
            } => Ok((entry, logical_deleted_rows)),
            Self::Rewrite {
                context,
                index,
                block,
                origin_stats,
                original_rows,
                logical_deleted_rows,
            } => {
                let serialized = context.block_builder.build(block, |block, generator| {
                    let granule_keys = generator.granule_cluster_key_offsets();
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    Ok((cluster_stats, block, granule_keys))
                })?;
                let extended_block_meta = match serialized {
                    BlockSerialization::Pending(pending) => {
                        BlockWriter::write_down(&context.block_builder.operator, pending).await?
                    }
                    BlockSerialization::Written(meta) => meta,
                };
                metrics_inc_replace_block_number_write(1);
                metrics_inc_replace_row_number_write(extended_block_meta.block_meta.row_count);
                metrics_inc_replace_replaced_blocks_rows(original_rows as u64);
                Ok((
                    MutationLogEntry::ReplacedBlock {
                        index,
                        block_meta: Arc::new(extended_block_meta),
                    },
                    logical_deleted_rows,
                ))
            }
        }
    }

    pub(crate) fn needs_build(&self) -> bool {
        matches!(self, Self::Rewrite { .. })
    }
}

#[derive(Clone)]
pub struct ReplaceIntoMutatorParams {
    on_conflict_fields: Vec<OnConflictField>,
    bloom_filter_column_indexes: Vec<FieldIndex>,
    block_slots: Option<BlockSlotDescription>,
    read_settings: ReadSettings,
    block_builder: BlockBuilder,
}

impl ReplaceIntoMutatorParams {
    pub fn try_create(
        block_builder: BlockBuilder,
        on_conflict_fields: Vec<OnConflictField>,
        bloom_filter_column_indexes: Vec<FieldIndex>,
        block_slots: Option<BlockSlotDescription>,
    ) -> Result<Self> {
        let read_settings = ReadSettings::from_ctx(&block_builder.ctx)?;
        Ok(Self {
            on_conflict_fields,
            bloom_filter_column_indexes,
            block_slots,
            read_settings,
            block_builder,
        })
    }

    fn into_aggregation_context(
        self,
        table: &FuseTable,
        segment_locations: Vec<(SegmentIndex, Location)>,
    ) -> Result<Arc<AggregationContext>> {
        let ctx = self.block_builder.ctx.clone();
        let data_accessor = table.get_operator();
        let table_schema = table.schema_with_stream();
        let write_settings = table.get_write_settings();

        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), table_schema.clone());
        let key_column_field_indexes: Vec<FieldIndex> = self
            .on_conflict_fields
            .iter()
            .map(|field| field.field_index)
            .collect();
        let mut remain_column_field_ids = Vec::new();
        for (index, field) in table_schema.fields().iter().enumerate() {
            if matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))) {
                continue;
            }
            if !key_column_field_indexes.contains(&index) {
                remain_column_field_ids.push(index);
            }
        }

        let key_column_reader = BlockReader::create(
            ctx.clone(),
            data_accessor.clone(),
            table_schema.clone(),
            Projection::Columns(key_column_field_indexes),
            false,
        )?;
        let remain_column_reader = if remain_column_field_ids.is_empty() {
            None
        } else {
            Some(BlockReader::create(
                ctx.clone(),
                data_accessor.clone(),
                table_schema.clone(),
                Projection::Columns(remain_column_field_ids.clone()),
                false,
            )?)
        };
        let stream_ctx = if table.change_tracking_enabled() {
            Some(StreamContext::try_create(
                ctx.get_function_context()?,
                table_schema,
                table.get_table_info().ident.seq,
                true,
                false,
            )?)
        } else {
            None
        };

        Ok(Arc::new(AggregationContext {
            segment_locations: AHashMap::from_iter(segment_locations),
            block_slots_in_charge: self.block_slots,
            on_conflict_fields: self.on_conflict_fields,
            bloom_filter_column_indexes: self.bloom_filter_column_indexes,
            remain_column_field_ids,
            key_column_reader,
            remain_column_reader,
            data_accessor,
            write_settings,
            read_settings: self.read_settings,
            segment_reader,
            block_builder: self.block_builder,
            stream_ctx,
            block_meta_cache: CacheManager::instance().get_block_meta_cache(),
        }))
    }
}

// Apply MergeIntoOperations to segments
pub struct ReplaceIntoOperationAggregator {
    deletion_accumulator: DeletionAccumulator,
    aggregation_ctx: Arc<AggregationContext>,
}

impl ReplaceIntoOperationAggregator {
    pub(crate) fn try_create(
        table: &FuseTable,
        params: ReplaceIntoMutatorParams,
        segment_locations: Vec<(SegmentIndex, Location)>,
    ) -> Result<Self> {
        let aggregation_ctx = params.into_aggregation_context(table, segment_locations)?;
        Ok(Self {
            deletion_accumulator: DeletionAccumulator::default(),
            aggregation_ctx,
        })
    }
}

// aggregate mutations (currently, deletion only)
impl ReplaceIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, replace_into_operation: ReplaceIntoOperation) -> Result<()> {
        let aggregation_ctx = &self.aggregation_ctx;
        metrics_inc_replace_number_accumulated_merge_action();

        let start = Instant::now();
        match replace_into_operation {
            ReplaceIntoOperation::Delete(partitions) => {
                for (segment_index, (path, ver)) in &aggregation_ctx.segment_locations {
                    // segment level
                    let load_param = LoadParams {
                        location: path.clone(),
                        len_hint: None,
                        ver: *ver,
                        put_cache: true,
                    };
                    let compact_segment_info =
                        aggregation_ctx.segment_reader.read(&load_param).await?;
                    let mut segment_info: Option<SegmentInfo> = None;

                    for DeletionByColumn {
                        columns_min_max,
                        key_hashes,
                        bloom_hashes,
                    } in &partitions
                    {
                        if aggregation_ctx
                            .overlapped(&compact_segment_info.summary.col_stats, columns_min_max)
                        {
                            let seg = match &segment_info {
                                None => {
                                    // un-compact the segment if necessary
                                    segment_info = Some(compact_segment_info.clone().try_into()?);
                                    segment_info.as_ref().unwrap()
                                }
                                Some(v) => v,
                            };

                            // block level pruning, using range index
                            for (block_index, block_meta) in seg.blocks.iter().enumerate() {
                                if let Some(BlockSlotDescription { num_slots, slot }) =
                                    &aggregation_ctx.block_slots_in_charge
                                {
                                    if block_index % num_slots != *slot as usize {
                                        // skip this block
                                        continue;
                                    }
                                }
                                if aggregation_ctx
                                    .overlapped(&block_meta.col_stats, columns_min_max)
                                {
                                    self.deletion_accumulator.add_block_deletion(
                                        *segment_index,
                                        block_index,
                                        key_hashes,
                                        bloom_hashes,
                                    )
                                }
                            }
                        }
                    }
                }
            }
            ReplaceIntoOperation::None => {}
        }

        metrics_inc_replace_accumulated_merge_action_time_ms(start.elapsed().as_millis() as u64);
        Ok(())
    }
}

// apply the mutations and generate mutation log
impl ReplaceIntoOperationAggregator {
    #[async_backtrace::framed]
    pub(crate) async fn prepare_tasks(&mut self) -> Result<VecDeque<DataBlock>> {
        let block_meta_cache = &self.aggregation_ctx.block_meta_cache;

        metrics_inc_replace_number_apply_deletion();

        // track number of segments and blocks after pruning (per merge action application)
        {
            metrics_inc_replace_segment_number_after_pruning(
                self.deletion_accumulator.deletions.len() as u64,
            );

            let num_blocks_mutated = self
                .deletion_accumulator
                .deletions
                .values()
                .fold(0, |acc, blocks_may_have_row_deletion| {
                    acc + blocks_may_have_row_deletion.len()
                });

            metrics_inc_replace_block_number_after_pruning(num_blocks_mutated as u64);
        }

        let start = Instant::now();
        let aggregation_ctx = &self.aggregation_ctx;
        let mut tasks = Vec::new();
        let mut num_rows_mutated = 0;
        for (segment_idx, block_deletion) in self.deletion_accumulator.deletions.drain() {
            let (segment_path, ver) = self
                .aggregation_ctx
                .segment_locations
                .get(&segment_idx)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, segment (idx {}) not found, during applying mutation log",
                        segment_idx
                    ))
                })?;

            let load_param = LoadParams {
                location: segment_path.clone(),
                len_hint: None,
                ver: *ver,
                put_cache: true,
            };

            // Retain SegmentInfo to avoid repeatedly extracting it from CompactSegmentInfo later.
            let mut opt_segment_info: Option<SegmentInfo> = None;

            for (block_index, keys) in block_deletion {
                let block_cache_key = format!("{segment_path}-{block_index}");
                let block_meta = match block_meta_cache.get(&block_cache_key) {
                    Some(block_meta) => block_meta,
                    None => {
                        let block_meta = if let Some(segment_info) = &opt_segment_info {
                            segment_info.blocks[block_index].clone()
                        } else {
                            let compact_segment_info =
                                aggregation_ctx.segment_reader.read(&load_param).await?;
                            let segment_info: SegmentInfo = compact_segment_info.try_into()?;
                            let block_meta = segment_info.blocks[block_index].clone();
                            opt_segment_info = Some(segment_info);
                            block_meta
                        };
                        // A query node typically processes only a subset of the BlockMeta in a given segment.
                        // Therefore, even though all BlockMeta of a segment are available here, not all are populated into the cache.
                        block_meta_cache.insert(block_cache_key, block_meta.as_ref().clone());
                        block_meta
                    }
                };

                num_rows_mutated += block_meta.row_count;
                tasks.push((
                    BlockMetaIndex {
                        segment_idx,
                        block_idx: block_index,
                    },
                    block_meta,
                    keys,
                ));
            }
        }
        if num_rows_mutated > 0 {
            metrics_inc_replace_row_number_after_pruning(num_rows_mutated);
        }

        if tasks.is_empty() {
            metrics_inc_replace_apply_deletion_time_ms(start.elapsed().as_millis() as u64);
            return Ok(VecDeque::new());
        }

        let batch = ReplaceMutationBatch::new(tasks.len(), start);
        Ok(tasks
            .into_iter()
            .map(|(index, block_meta, (deleted_key_hashes, bloom_hashes))| {
                DataBlock::empty_with_meta(Box::new(ReplaceBlockMutationTask {
                    context: self.aggregation_ctx.clone(),
                    index,
                    block_meta,
                    deleted_key_hashes,
                    bloom_hashes,
                    batch: batch.clone(),
                }))
            })
            .collect())
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_deletion_to_data_block(
        self: &Arc<Self>,
        index: BlockMetaIndex,
        block_meta: &BlockMeta,
        deleted_key_hashes: &(ahash::HashSet<UniqueKeyDigest>, Vec<Vec<u64>>),
    ) -> Result<Option<PreparedReplaceMutation>> {
        let (deleted_key_hashes, bloom_hashes) = deleted_key_hashes;
        info!(
            "apply delete to segment idx {}, block idx {}, num of deletion key hashes: {}",
            index.segment_idx,
            index.block_idx,
            deleted_key_hashes.len()
        );

        if block_meta.row_count == 0 {
            return Ok(None);
        }

        // apply bloom filter pruning if possible
        let pruned = self
            .apply_bloom_pruning(block_meta, bloom_hashes, &self.bloom_filter_column_indexes)
            .await;

        if pruned {
            // skip this block
            metrics_inc_replace_block_number_bloom_pruned(1);
            return Ok(None);
        }

        let key_columns_data = read_block(
            self.write_settings.storage_format,
            &self.key_column_reader,
            block_meta,
            &self.read_settings,
        )
        .await?;

        let num_rows = key_columns_data.num_rows();

        let on_conflict_fields = &self.on_conflict_fields;
        let mut columns = Vec::with_capacity(on_conflict_fields.len());
        for (field, _) in on_conflict_fields.iter().enumerate() {
            let on_conflict_field_index = field;
            let entry_value = key_columns_data
                .columns()
                .get(on_conflict_field_index)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, block entry (index {}) not found. segment index {}, block index {}",
                        on_conflict_field_index, index.segment_idx, index.block_idx
                    ))
                })?
                .value();
            columns.push(entry_value);
        }
        let columns: Vec<_> = columns.iter().collect();

        let mut bitmap = MutableBitmap::new();
        for row in 0..num_rows {
            if let Some(hash) = row_hash_of_columns(&columns, row)? {
                // some row hash means on-conflict columns of this row contains non-null values
                // let's check it out
                bitmap.push(!deleted_key_hashes.contains(&hash));
            } else {
                // otherwise, keep this row
                bitmap.push(true);
            }
        }

        let delete_nums = bitmap.null_count();
        info!("number of row deleted: {}", delete_nums);

        // shortcut: nothing to be deleted
        if delete_nums == 0 {
            info!("nothing deleted");
            metrics_inc_replace_block_of_zero_row_deleted(1);
            // nothing to be deleted
            return Ok(None);
        }

        // shortcut: whole block deletion
        if delete_nums == block_meta.row_count as usize {
            info!("whole block deletion");
            metrics_inc_replace_whole_block_deletion(1);
            metrics_inc_replace_deleted_blocks_rows(num_rows as u64);
            // whole block deletion
            // NOTE that if deletion marker is enabled, check the real meaning of `row_count`
            return Ok(Some(PreparedReplaceMutation::Log {
                entry: MutationLogEntry::DeletedBlock { index },
                logical_deleted_rows: delete_nums as u64,
            }));
        }

        let bitmap = bitmap.into();
        let mut key_columns_data_after_deletion = key_columns_data.filter_with_bitmap(&bitmap)?;

        let mut new_block = match &self.remain_column_reader {
            None => key_columns_data_after_deletion,
            Some(remain_columns_reader) => {
                metrics_inc_replace_block_number_totally_loaded(1);
                metrics_inc_replace_row_number_totally_loaded(block_meta.row_count);

                // read the remaining columns
                let remain_columns_data =
                    self.read_block(remain_columns_reader, block_meta).await?;

                // remove the deleted rows
                let remain_columns_data_after_deletion =
                    remain_columns_data.filter_with_bitmap(&bitmap)?;

                // merge the remaining columns
                key_columns_data_after_deletion.merge_block(remain_columns_data_after_deletion);

                // resort the block
                let col_indexes = self
                    .on_conflict_fields
                    .iter()
                    .map(|f| f.field_index)
                    .chain(self.remain_column_field_ids.iter().copied())
                    .collect::<Vec<_>>();
                let mut projection = (0..col_indexes.len()).collect::<Vec<_>>();
                projection.sort_by_key(|&i| col_indexes[i]);
                let func_ctx = self.block_builder.ctx.get_function_context()?;
                BlockOperator::Project { projection }
                    .execute(&func_ctx, key_columns_data_after_deletion)?
            }
        };

        if let Some(stream_ctx) = &self.stream_ctx {
            // generate row id column
            let mut row_ids = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                row_ids.push(i as u64);
            }
            let row_num =
                Column::filter(&UInt64Type::from_data(row_ids), &bitmap).wrap_nullable(None);
            new_block.add_column(row_num);

            let stream_meta = gen_mutation_stream_meta(None, &block_meta.location.0, 0)?;
            new_block = stream_ctx.apply(new_block, &stream_meta)?;
        }

        Ok(Some(PreparedReplaceMutation::Rewrite {
            context: self.clone(),
            index,
            block: new_block,
            origin_stats: block_meta.cluster_stats.clone(),
            original_rows: num_rows,
            logical_deleted_rows: delete_nums as u64,
        }))
    }

    fn overlapped(
        &self,
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        Self::check_overlap(&self.on_conflict_fields, column_stats, columns_min_max)
    }

    // if any item of `column_min_max` does NOT overlap with the corresponding item of `column_stats`
    // returns false, otherwise returns true.
    fn check_overlap(
        on_conflict_fields: &[OnConflictField],
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        for (idx, field) in on_conflict_fields.iter().enumerate() {
            let column_id = field.table_field.column_id();
            let (min, max) = &columns_min_max[idx];
            if !Self::check_overlapped_by_stats(column_stats.get(&column_id), min, max) {
                return false;
            }
        }
        true
    }

    fn check_overlapped_by_stats(
        column_stats: Option<&ColumnStatistics>,
        key_min: &Scalar,
        key_max: &Scalar,
    ) -> bool {
        if let Some(stats) = column_stats {
            let max = stats.max();
            let min = stats.min();
            std::cmp::min(key_max, max) >= std::cmp::max(key_min, min)
                || // coincide overlap
                (max == key_max && min == key_min)
        } else {
            // if column range index does not exist, assume overlapped
            true
        }
    }

    async fn read_block(&self, reader: &BlockReader, block_meta: &BlockMeta) -> Result<DataBlock> {
        let merged_io_read_result = reader
            .read_columns_data_by_merge_io(
                &self.read_settings,
                &block_meta.location.0,
                &block_meta.col_metas,
                &None,
            )
            .await?;

        // deserialize block data
        // cpu intensive task, send them to dedicated thread pool
        let storage_format = self.write_settings.storage_format;
        let block_meta_ptr = block_meta.clone();
        let reader = reader.clone();
        GlobalIORuntime::instance()
            .spawn(async move {
                let column_chunks = merged_io_read_result.columns_chunks()?;
                reader.deserialize_chunks(
                    block_meta_ptr.location.0.as_str(),
                    block_meta_ptr.row_count as usize,
                    &block_meta_ptr.compression,
                    &block_meta_ptr.col_metas,
                    column_chunks,
                    &storage_format,
                )
            })
            .await
            .map_err(|e| {
                ErrorCode::Internal(
                    "unexpected, failed to join aggregation context read block tasks for replace into.",
                )
                    .add_message_back(e.to_string())
            })?
    }

    // return true if the block is pruned, otherwise false
    async fn apply_bloom_pruning(
        &self,
        block_meta: &BlockMeta,
        input_hashes: &[Vec<u64>],
        bloom_on_conflict_field_index: &[FieldIndex],
    ) -> bool {
        if bloom_on_conflict_field_index.is_empty() {
            return false;
        }
        if let Some(loc) = &block_meta.bloom_filter_index_location {
            match self
                .load_bloom_filter(
                    loc,
                    block_meta.bloom_filter_index_size,
                    bloom_on_conflict_field_index,
                )
                .await
            {
                Ok(filters) => {
                    // the caller ensures that the input_hashes is not empty
                    let row_count = input_hashes[0].len();

                    // let assume that the target block is prunable
                    let mut block_pruned = true;
                    for row in 0..row_count {
                        // for each row, by default, assume that columns of this row do have conflict with the target block.
                        let mut row_not_prunable = true;
                        for (col_idx, col_hash) in input_hashes.iter().enumerate() {
                            // For each column of current row, check if the corresponding bloom
                            // filter contains the digest of the column.
                            //
                            // Any one of the columns NOT contains by the corresponding bloom filter,
                            // indicates that the row is prunable(thus, we do not stop on the first column that
                            // the bloom filter contains).

                            // - if bloom filter presents, check if the column is contained
                            // - if bloom filter absents, do nothing(since by default, we assume that the row is not-prunable)
                            if let Some(col_filter) = &filters[col_idx] {
                                let hash = col_hash[row];
                                if hash == 0 || !col_filter.contains_digest(hash) {
                                    // - hash == 0 indicates that the column value is null, which equals nothing.
                                    // - NOT `contains_digest`, indicates that this column of row does not match
                                    row_not_prunable = false;
                                    // if one column not match, we do not need to check other columns
                                    break;
                                }
                            }
                        }
                        if row_not_prunable {
                            // any row not prunable indicates that the target block is not prunable
                            block_pruned = false;
                            break;
                        }
                    }
                    block_pruned
                }
                Err(e) => {
                    // broken index should not stop us:
                    warn!("failed to build bloom index column name: {}", e);
                    // failed to load bloom filter, do not prune
                    false
                }
            }
        } else {
            // no bloom filter, no pruning
            false
        }
    }

    async fn load_bloom_filter(
        &self,
        location: &Location,
        index_len: u64,
        bloom_on_conflict_field_index: &[FieldIndex],
    ) -> Result<Vec<Option<Arc<FilterImpl>>>> {
        // different block may have different version of bloom filter index
        let mut col_names = Vec::with_capacity(bloom_on_conflict_field_index.len());

        for idx in bloom_on_conflict_field_index {
            let bloom_column_name = BloomIndex::build_filter_bloom_name(
                location.1,
                &self.on_conflict_fields[*idx].table_field,
            )?;
            col_names.push(bloom_column_name);
        }

        // using load_bloom_filter_by_columns is attractive,
        // but it do not care about the version of the bloom filter index
        let block_filter = location
            .read_block_filter(
                self.data_accessor.clone(),
                &self.read_settings,
                &col_names,
                index_len,
            )
            .await?;

        // reorder the filter according to the order of bloom_on_conflict_field
        let mut filters = Vec::with_capacity(bloom_on_conflict_field_index.len());
        for filter_col_name in &col_names {
            match block_filter.filter_schema.index_of(filter_col_name) {
                Ok(idx) => {
                    filters.push(Some(block_filter.filters[idx].clone()));
                }
                Err(_) => {
                    info!(
                        "bloom filter column {} not found for block {}",
                        filter_col_name, location.0
                    );
                    filters.push(None);
                }
            }
        }

        Ok(filters)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::Value;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::nullable::NullableColumn;
    use databend_common_expression::types::number::Int32Type;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::*;

    #[test]
    fn test_deletion_accumulator_is_fragment_independent() {
        let mut first = DeletionAccumulator::default();
        let mut second = DeletionAccumulator::default();
        // Different input blocks may carry the same candidate block. The accumulator must
        // merge their keys and Bloom digests instead of making the result fragment-dependent.
        let keys_a = ahash::HashSet::from_iter([1, 2]);
        let keys_b = ahash::HashSet::from_iter([2, 3]);
        let bloom_a = vec![vec![10, 20], vec![100, 200]];
        let bloom_b = vec![vec![20, 30], vec![200, 300]];

        first.add_block_deletion(4, 7, &keys_a, &bloom_a);
        first.add_block_deletion(4, 7, &keys_b, &bloom_b);
        second.add_block_deletion(4, 7, &keys_b, &bloom_b);
        second.add_block_deletion(4, 7, &keys_a, &bloom_a);

        assert_eq!(first.deletions[&4][&7].0, second.deletions[&4][&7].0);
        assert_eq!(
            first.deletions[&4][&7].0,
            ahash::HashSet::from_iter([1, 2, 3])
        );
        for column in 0..2 {
            let mut first_hashes = first.deletions[&4][&7].1[column].clone();
            let mut second_hashes = second.deletions[&4][&7].1[column].clone();
            first_hashes.sort_unstable();
            second_hashes.sort_unstable();
            assert_eq!(first_hashes, second_hashes);
            assert_eq!(first_hashes.len(), 4);
        }
    }

    #[test]
    fn test_overlap_keeps_boundary_and_missing_statistics() {
        let statistics = ColumnStatistics::new(
            Scalar::Number(NumberScalar::Int32(10)),
            Scalar::Number(NumberScalar::Int32(20)),
            0,
            0,
            None,
        );
        let boundary = Scalar::Number(NumberScalar::Int32(20));
        let outside = Scalar::Number(NumberScalar::Int32(21));

        assert!(AggregationContext::check_overlapped_by_stats(
            Some(&statistics),
            &boundary,
            &boundary,
        ));
        assert!(!AggregationContext::check_overlapped_by_stats(
            Some(&statistics),
            &outside,
            &outside,
        ));
        assert!(AggregationContext::check_overlapped_by_stats(
            None, &outside, &outside,
        ));
    }

    #[test]
    fn test_nullable_keys_are_not_deletion_candidates() -> Result<()> {
        let nullable = NullableColumn::new_column(
            Int32Type::from_data(vec![1_i32, 2_i32]),
            [true, false].into_iter().collect(),
        );
        let value = Value::Column(nullable);
        assert!(row_hash_of_columns(&[&value], 0)?.is_some());
        assert!(row_hash_of_columns(&[&value], 1)?.is_none());
        Ok(())
    }

    #[test]
    fn test_check_overlap() -> Result<()> {
        // setup :
        //
        // - on conflict('xx_id', 'xx_type', 'xx_time');
        //
        // - range index of columns
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]

        // setup schema
        let field_type_id = TableDataType::Number(NumberDataType::UInt64);
        let field_type_string = TableDataType::String;
        let field_type_time = TableDataType::Number(NumberDataType::UInt32);

        let xx_id = TableField::new("xx_id", field_type_id);
        let xx_type = TableField::new("xx_type", field_type_string);
        let xx_time = TableField::new("xx_time", field_type_time);

        let schema = TableSchema::new(vec![xx_id, xx_type, xx_time]);

        let fields = schema.fields();

        // setup the ON CONFLICT fields
        let on_conflict_fields = fields
            .iter()
            .enumerate()
            .map(|(id, field)| OnConflictField {
                table_field: field.clone(),
                field_index: id,
            })
            .collect::<Vec<_>>();

        // set up range index of columns
        // the null_count/in_memory_size/distinct_of_values do not matter in this case
        let range = |min: Scalar, max: Scalar| ColumnStatistics::new(min, max, 0, 0, None);

        let column_range_indexes = HashMap::from_iter([
            // range of xx_id [1, 10]
            (
                0,
                range(
                    Scalar::Number(NumberScalar::UInt64(1)),
                    Scalar::Number(NumberScalar::UInt64(10)),
                ),
            ),
            // range of xx_type [a, z]
            (
                1,
                range(
                    Scalar::String("a".to_string()),
                    Scalar::String("z".to_string()),
                ),
            ),
            // range of xx_time [100, 200]
            (
                2,
                range(
                    Scalar::Number(NumberScalar::UInt32(100)),
                    Scalar::Number(NumberScalar::UInt32(200)),
                ),
            ),
        ]);

        // case 1:
        //
        // - min/max of input block
        //
        //  'xx_id' : [1, 9]
        //  'xx_type' : ["b", "y"]
        //  'xx_time' : [101, 200]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == true
        //   since value of all the ON CONFLICT columns of input block overlap with range index

        let input_column_min_max = [
            // for xx_id column, overlaps
            (
                Scalar::Number(NumberScalar::UInt64(1)),
                Scalar::Number(NumberScalar::UInt64(9)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string()),
                Scalar::String("y".to_string()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(101)),
                Scalar::Number(NumberScalar::UInt32(200)),
            ),
        ];

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(overlap);

        // case 2:
        //
        // - min/max of input block
        //
        //  'xx_id' : [11, 12]
        //  'xx_type' : ["b", "b"]
        //  'xx_time' : [100, 100]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == false
        //
        //   although columns 'xx_type' and 'xx_time' do overlap, but 'xx_id' does not overlap,
        //   so the result is NOT overlap

        let input_column_min_max = [
            // for xx_id column, NOT overlaps
            (
                Scalar::Number(NumberScalar::UInt64(11)),
                Scalar::Number(NumberScalar::UInt64(12)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string()),
                Scalar::String("b".to_string()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(100)),
                Scalar::Number(NumberScalar::UInt32(100)),
            ),
        ];

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(!overlap);

        // case 3: (column rang index not exist)
        //
        // - min/max of input block
        //
        //  'xx_id' : [11, 12]
        //  'xx_type' : ["b", "b"]
        //  'xx_time' : [100, 100]
        //
        // - the range index of columns are (after tweaks)
        //
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == true
        //
        //   range index of column 'xx_id' does not exist (explicitly removed)
        //   the result should be overlapped

        let input_column_min_max = [
            // for xx_id column, NOT overlaps
            (
                Scalar::Number(NumberScalar::UInt64(11)),
                Scalar::Number(NumberScalar::UInt64(12)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string()),
                Scalar::String("b".to_string()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(100)),
                Scalar::Number(NumberScalar::UInt32(100)),
            ),
        ];

        let column_range_indexes = {
            let mut cloned = column_range_indexes;
            cloned.remove(&0); // remove range index of col xx_id
            cloned
        };

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(overlap);

        Ok(())
    }
}
