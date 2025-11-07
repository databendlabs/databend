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
use std::sync::Arc;

use async_channel::Sender;
use chrono::DateTime;
use databend_common_base::base::tokio::sync::OwnedSemaphorePermit;
use databend_common_catalog::plan::block_id_in_segment;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnMetaV0;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use futures_util::future;

use super::PrunedColumnOrientedSegmentMeta;
use crate::pruning::BlockPruner;
use crate::FuseBlockPartInfo;

pub struct ColumnOrientedBlockPruneSink {
    block_pruner: Arc<BlockPruner>,
    column_ids: Vec<ColumnId>,
    sender: Option<Sender<Result<PartInfoPtr>>>,
}

impl ColumnOrientedBlockPruneSink {
    pub fn create(
        input: Arc<InputPort>,
        block_pruner: Arc<BlockPruner>,
        sender: Sender<Result<PartInfoPtr>>,
        column_ids: Vec<ColumnId>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            ColumnOrientedBlockPruneSink {
                block_pruner,
                column_ids,
                sender: Some(sender),
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for ColumnOrientedBlockPruneSink {
    const NAME: &'static str = "ColumnOrientedBlockPruneSink";

    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    async fn consume(&mut self, mut data: DataBlock) -> Result<bool> {
        let ptr = data.take_meta().ok_or_else(|| {
            ErrorCode::Internal("Cannot downcast meta to PrunedColumnOrientedSegmentMeta")
        })?;
        let (segment_location, segment) = PrunedColumnOrientedSegmentMeta::downcast_from(ptr)
            .ok_or_else(|| {
                ErrorCode::Internal("Cannot downcast meta to PrunedColumnOrientedSegmentMeta")
            })?
            .segments;

        let range_pruner = &self.block_pruner.pruning_ctx.range_pruner;
        let bloom_pruner = &self.block_pruner.pruning_ctx.bloom_pruner;

        let block_num = segment.block_metas.num_rows();
        let location_path_col = segment.location_path_col();
        let compression_col = segment.compression_col();
        let create_on_col = segment.col_by_name(&[CREATE_ON]).unwrap();
        let bloom_index_location_col = segment.col_by_name(&[BLOOM_FILTER_INDEX_LOCATION]).unwrap();
        let bloom_index_size_col = segment.bloom_filter_index_size_col();
        let block_size_col = segment.block_size_col();
        let row_count_col = segment.row_count_col();

        let pruning_runtime = &self.block_pruner.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.block_pruner.pruning_ctx.pruning_semaphore;

        let mut pruning_tasks = Vec::with_capacity(block_num);

        for block_idx in 0..block_num {
            let location_path = location_path_col.index(block_idx).unwrap().to_string();

            // Skip blocks that don't pass internal column pruning
            if self
                .block_pruner
                .pruning_ctx
                .internal_column_pruner
                .as_ref()
                .is_some_and(|pruner| !pruner.should_keep(BLOCK_NAME_COL_NAME, &location_path))
            {
                continue;
            }

            // Clone necessary data for the async task
            let column_ids = self.column_ids.clone();
            let segment = segment.clone();
            let segment_location = segment_location.clone();
            let range_pruner = range_pruner.clone();
            let bloom_pruner = bloom_pruner.clone();
            let sender = self.sender.as_ref().unwrap().clone();
            let location_path = location_path.clone();
            let compression_col = compression_col.clone();
            let block_size_col = block_size_col.clone();
            let row_count_col = row_count_col.clone();
            let create_on_col = create_on_col.clone();
            let bloom_index_location_col = bloom_index_location_col.clone();
            let bloom_index_size_col = bloom_index_size_col.clone();

            pruning_tasks.push(move |permit: OwnedSemaphorePermit| {
                Box::pin(async move {
                    let _permit = permit;
                    // 2. prune columns by range index
                    let mut columns_stat = HashMap::with_capacity(column_ids.len());
                    let mut columns_meta = HashMap::with_capacity(column_ids.len());

                    for column_id in &column_ids {
                        if let Some(stat) = segment.stat_col(*column_id) {
                            let stat = stat.index(block_idx).unwrap();
                            let stat = stat.as_tuple().unwrap();
                            let min = stat[0].to_owned();
                            let max = stat[1].to_owned();
                            let null_count = stat[2].as_number().unwrap().as_u_int64().unwrap();
                            let in_memory_size = stat[3].as_number().unwrap().as_u_int64().unwrap();
                            let distinct_of_values = match stat[4] {
                                ScalarRef::Number(number_scalar) => {
                                    Some(*number_scalar.as_u_int64().unwrap())
                                }
                                ScalarRef::Null => None,
                                _ => unreachable!(),
                            };
                            columns_stat.insert(
                                *column_id,
                                ColumnStatistics::new(
                                    min,
                                    max,
                                    *null_count,
                                    *in_memory_size,
                                    distinct_of_values,
                                ),
                            );
                        }
                    }

                    if !range_pruner.should_keep(&columns_stat, None) {
                        return Ok::<_, ()>(());
                    }

                    let row_count = row_count_col[block_idx];
                    let compression = Compression::from_u8(compression_col[block_idx]);
                    let block_size = block_size_col[block_idx];

                    // Bloom filter pruning
                    if let Some(bloom_pruner) = bloom_pruner {
                        let location_scalar = bloom_index_location_col.index(block_idx).unwrap();
                        let index_location = match location_scalar {
                            ScalarRef::Null => None,
                            ScalarRef::Tuple(tuple) => {
                                if tuple.len() != 2 {
                                    unreachable!()
                                }
                                Some((
                                    tuple[0].as_string().unwrap().to_string(),
                                    *tuple[1].as_number().unwrap().as_u_int64().unwrap(),
                                ))
                            }
                            _ => unreachable!(),
                        };
                        let index_size = bloom_index_size_col[block_idx];

                        // used to rebuild bloom index
                        let block_read_info = BlockReadInfo {
                            location: location_path.clone(),
                            row_count,
                            col_metas: columns_meta.clone(),
                            compression,
                            block_size,
                        };

                        if !bloom_pruner
                            .should_keep(
                                &index_location,
                                index_size,
                                &columns_stat,
                                column_ids.clone(),
                                &block_read_info,
                            )
                            .await
                        {
                            return Ok(());
                        }
                    }

                    // Get create_on value
                    let create_on = create_on_col.index(block_idx).unwrap();
                    let create_on = match create_on {
                        ScalarRef::Null => None,
                        ScalarRef::Number(number_scalar) => Some(
                            DateTime::from_timestamp(*number_scalar.as_int64().unwrap(), 0)
                                .unwrap(),
                        ),
                        _ => unreachable!(),
                    };

                    let block_meta_index = BlockMetaIndex {
                        segment_idx: segment_location.segment_idx,
                        block_idx,
                        range: None,
                        page_size: row_count as usize,
                        block_id: block_id_in_segment(block_num, block_idx),
                        block_location: location_path.clone(),
                        segment_location: segment_location.location.0.clone(),
                        snapshot_location: segment_location.snapshot_loc.clone(),
                        matched_rows: None,
                        vector_scores: None,
                        virtual_block_meta: None,
                    };

                    // Collect column metadata
                    for column_id in &column_ids {
                        if let Some(meta) = segment.meta_col(*column_id) {
                            let meta = meta.index(block_idx).unwrap();
                            let meta = meta.as_tuple().unwrap();
                            let offset = meta[0].as_number().unwrap().as_u_int64().unwrap();
                            let length = meta[1].as_number().unwrap().as_u_int64().unwrap();
                            let num_values = meta[2].as_number().unwrap().as_u_int64().unwrap();
                            columns_meta.insert(
                                *column_id,
                                ColumnMeta::Parquet(ColumnMetaV0 {
                                    offset: *offset,
                                    len: *length,
                                    num_values: *num_values,
                                }),
                            );
                        }
                    }

                    let part_info = FuseBlockPartInfo::create(
                        location_path,
                        row_count,
                        columns_meta,
                        Some(columns_stat),
                        compression,
                        None, // TODO(Sky): sort_min_max
                        Some(block_meta_index),
                        create_on,
                    );

                    let _ = sender.send(Ok(part_info)).await;
                    Ok(())
                })
            });
        }

        // Execute all pruning tasks in parallel
        if !pruning_tasks.is_empty() {
            let join_handlers = pruning_runtime
                .try_spawn_batch_with_owned_semaphore(pruning_semaphore.clone(), pruning_tasks)
                .await
                .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;

            // Wait for all tasks to complete
            let _ = future::try_join_all(join_handlers)
                .await
                .map_err(|e| ErrorCode::StorageOther(format!("block pruning failure, {}", e)))?;
        }
        Ok(false)
    }
}
