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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_base::base::tokio::sync::Semaphore;
use common_base::base::ProgressValues;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::plan::split_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::NumberColumn;
use common_expression::BlockMetaInfoDowncast;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_storage::metrics::merge_into::metrics_inc_merge_into_accumulate_milliseconds;
use common_storage::metrics::merge_into::metrics_inc_merge_into_apply_milliseconds;
use common_storage::metrics::merge_into::metrics_inc_merge_into_deleted_blocks_counter;
use common_storage::metrics::merge_into::metrics_inc_merge_into_deleted_blocks_rows_counter;
use common_storage::metrics::merge_into::metrics_inc_merge_into_replace_blocks_counter;
use common_storage::metrics::merge_into::metrics_inc_merge_into_replace_blocks_rows_counter;
use itertools::Itertools;
use log::info;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::WriteSettings;
use crate::operations::acquire_task_permit;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::merge_into::processors::RowIdKind;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::read_block;
use crate::operations::BlockMetaIndex;

struct AggregationContext {
    ctx: Arc<dyn TableContext>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    block_builder: BlockBuilder,
    block_reader: Arc<BlockReader>,
}

pub struct MatchedAggregator {
    io_request_semaphore: Arc<Semaphore>,
    segment_reader: CompactSegmentInfoReader,
    segment_locations: AHashMap<SegmentIndex, Location>,
    block_mutation_row_offset: HashMap<u64, (HashSet<usize>, HashSet<usize>)>,
    aggregation_ctx: Arc<AggregationContext>,
    entries: Vec<MutationLogEntry>,
    distributed_recieve: bool,
}

impl MatchedAggregator {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        target_table_schema: TableSchemaRef,
        data_accessor: Operator,
        write_settings: WriteSettings,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
        segment_locations: Vec<(SegmentIndex, Location)>,
        distributed_recieve: bool,
    ) -> Result<Self> {
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), target_table_schema.clone());

        let block_reader = {
            let projection =
                Projection::Columns((0..target_table_schema.num_fields()).collect_vec());
            BlockReader::create(
                data_accessor.clone(),
                target_table_schema,
                projection,
                ctx.clone(),
                false,
            )
        }?;

        Ok(Self {
            aggregation_ctx: Arc::new(AggregationContext {
                ctx,
                write_settings,
                read_settings,
                data_accessor,
                block_builder,
                block_reader,
            }),
            io_request_semaphore,
            segment_reader,
            block_mutation_row_offset: HashMap::new(),
            segment_locations: AHashMap::from_iter(segment_locations.into_iter()),
            entries: Vec::new(),
            distributed_recieve,
        })
    }

    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, data_block: DataBlock) -> Result<()> {
        // we need to distinct MutationLogs and RowIds
        // this operation is lightweight.
        let logs = MutationLogs::try_from(data_block.clone());
        if self.distributed_recieve && logs.is_ok() {
            self.entries.extend(logs.unwrap().entries);
            return Ok(());
        }

        if data_block.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        // data_block is from matched_split, so there is only one column.
        // that's row_id
        let row_ids = get_row_id(&data_block, 0)?;
        let row_id_kind = RowIdKind::downcast_ref_from(data_block.get_meta().unwrap()).unwrap();
        match row_id_kind {
            RowIdKind::Update => {
                for row_id in row_ids {
                    let (prefix, offset) = split_row_id(row_id);
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
                for row_id in row_ids {
                    let (prefix, offset) = split_row_id(row_id);
                    // support idempotent delete
                    self.block_mutation_row_offset
                        .entry(prefix)
                        .or_insert_with(|| (HashSet::new(), HashSet::new()))
                        .1
                        .insert(offset as usize);
                }
            }
        };
        let elapsed_time = start.elapsed().as_millis() as u64;
        metrics_inc_merge_into_accumulate_milliseconds(elapsed_time);
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        let start = Instant::now();
        // 1.get modified segments
        let mut segment_infos = HashMap::<SegmentIndex, SegmentInfo>::new();

        for prefix in self.block_mutation_row_offset.keys() {
            let (segment_idx, _) = split_prefix(*prefix);
            let segment_idx = segment_idx as usize;
            if let Entry::Vacant(e) = segment_infos.entry(segment_idx) {
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

                let compact_segment_info = self.segment_reader.read(&load_param).await?;
                let segment_info: SegmentInfo = compact_segment_info.try_into()?;

                e.insert(segment_info);
            } else {
                continue;
            }
        }

        let io_runtime = GlobalIORuntime::instance();
        let mut mutation_log_handlers = Vec::with_capacity(self.block_mutation_row_offset.len());

        for item in &self.block_mutation_row_offset {
            let (segment_idx, block_idx) = split_prefix(*item.0);
            let segment_idx = segment_idx as usize;
            let permit = acquire_task_permit(self.io_request_semaphore.clone()).await?;
            let aggregation_ctx = self.aggregation_ctx.clone();
            let segment_info = segment_infos.get(&segment_idx).unwrap();
            info!(
                "merge into apply: segment_idx:{},blk_idx:{}",
                segment_idx, block_idx
            );
            let block_idx = segment_info.blocks.len() - block_idx as usize - 1;
            assert!(block_idx < segment_info.blocks.len());
            // the row_id is generated by block_id, not block_idx,reference to fill_internal_column_meta()
            let block_meta = segment_info.blocks[block_idx].clone();

            let update_modified_offsets = &item.1.0;
            let delete_modified_offsets = &item.1.1;
            let modified_offsets: HashSet<usize> = update_modified_offsets
                .union(delete_modified_offsets)
                .cloned()
                .collect();

            if modified_offsets.len()
                < update_modified_offsets.len() + delete_modified_offsets.len()
            {
                return Err(ErrorCode::UnresolvableConflict(
                    "multi rows from source match one and the same row in the target_table multi times",
                ));
            }
            let handle = io_runtime.spawn(async_backtrace::location!().frame({
                async move {
                    let mutation_log_entry = aggregation_ctx
                        .apply_update_and_deletion_to_data_block(
                            segment_idx,
                            block_idx,
                            &block_meta,
                            modified_offsets,
                        )
                        .await?;

                    drop(permit);
                    Ok::<_, ErrorCode>(mutation_log_entry)
                }
            }));
            mutation_log_handlers.push(handle);
        }

        let log_entries = futures::future::try_join_all(mutation_log_handlers)
            .await
            .map_err(|e| {
                ErrorCode::Internal("unexpected, failed to join apply-deletion tasks.")
                    .add_message_back(e.to_string())
            })?;
        let mut mutation_logs: Vec<MutationLogEntry> = self.entries.drain(..).into_iter().collect();
        for maybe_log_entry in log_entries {
            if let Some(segment_mutation_log) = maybe_log_entry? {
                mutation_logs.push(segment_mutation_log);
            }
        }
        let elapsed_time = start.elapsed().as_millis() as u64;
        metrics_inc_merge_into_apply_milliseconds(elapsed_time);
        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_update_and_deletion_to_data_block(
        &self,
        segment_idx: SegmentIndex,
        block_idx: BlockIndex,
        block_meta: &BlockMeta,
        modified_offsets: HashSet<usize>,
    ) -> Result<Option<MutationLogEntry>> {
        info!(
            "apply update and delete to segment idx {}, block idx {}",
            segment_idx, block_idx,
        );
        let progress_values = ProgressValues {
            rows: modified_offsets.len(),
            bytes: 0,
        };
        self.ctx.get_write_progress().incr(&progress_values);
        let origin_data_block = read_block(
            self.write_settings.storage_format,
            &self.block_reader,
            block_meta,
            &self.read_settings,
        )
        .await?;
        let origin_num_rows = origin_data_block.num_rows();
        // apply delete
        let mut bitmap = MutableBitmap::new();
        for row in 0..origin_num_rows {
            if modified_offsets.contains(&row) {
                bitmap.push(false);
            } else {
                bitmap.push(true);
            }
        }
        let res_block = origin_data_block.filter_with_bitmap(&bitmap.into())?;

        if res_block.is_empty() {
            metrics_inc_merge_into_deleted_blocks_counter(1);
            metrics_inc_merge_into_deleted_blocks_rows_counter(origin_num_rows as u32);
            return Ok(Some(MutationLogEntry::DeletedBlock {
                index: BlockMetaIndex {
                    segment_idx,
                    block_idx,
                },
            }));
        }

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let origin_stats = block_meta.cluster_stats.clone();
        let serialized = GlobalIORuntime::instance()
            .spawn_blocking(move || {
                block_builder.build(res_block, |block, generator| {
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    info!(
                        "serialize block after get cluster_stats:\n {:?}",
                        cluster_stats
                    );
                    Ok((cluster_stats, block))
                })
            })
            .await?;

        // persistent data
        let new_block_meta = serialized.block_meta;
        let new_block_location = new_block_meta.location.0.clone();
        let new_block_raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        write_data(new_block_raw_data, &data_accessor, &new_block_location).await?;

        metrics_inc_merge_into_replace_blocks_counter(1);
        metrics_inc_merge_into_replace_blocks_rows_counter(origin_num_rows as u32);
        // generate log
        let mutation = MutationLogEntry::ReplacedBlock {
            index: BlockMetaIndex {
                segment_idx,
                block_idx,
            },
            block_meta: Arc::new(new_block_meta),
        };

        Ok(Some(mutation))
    }
}

pub(crate) fn get_row_id(data_block: &DataBlock, row_id_idx: usize) -> Result<Buffer<u64>> {
    let row_id_col = data_block.get_by_offset(row_id_idx);
    match row_id_col.value.as_column() {
        Some(Column::Nullable(boxed)) => match &boxed.column {
            Column::Number(NumberColumn::UInt64(data)) => Ok(data.clone()),
            _ => Err(ErrorCode::BadArguments("row id is not uint64")),
        },
        _ => Err(ErrorCode::BadArguments("row id is not uint64")),
    }
}
