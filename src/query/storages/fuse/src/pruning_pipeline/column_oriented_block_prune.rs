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
use databend_common_catalog::plan::block_id_in_segment;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnMetaV0;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;

use super::PrunedColumnOrientedSegmentMeta;
use crate::pruning::BlockPruner;
use crate::FuseBlockPartInfo;
use crate::COMPRESSION;
use crate::CREATE_ON;
use crate::LOCATION;
use crate::LOCATION_PATH;
use crate::ROW_COUNT;

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

        let block_num = segment.block_metas.num_rows();
        let location_path_col = segment.col_by_name(&[LOCATION, LOCATION_PATH]).unwrap();
        let rows_count_col = segment.col_by_name(&[ROW_COUNT]).unwrap();
        let compression_col = segment.col_by_name(&[COMPRESSION]).unwrap();
        let create_on_col = segment.col_by_name(&[CREATE_ON]).unwrap();

        for block_idx in 0..block_num {
            // 1. prune internal column
            let location_path = location_path_col.index(block_idx).unwrap();
            let location_path = location_path.as_string().unwrap();
            if self
                .block_pruner
                .pruning_ctx
                .internal_column_pruner
                .as_ref()
                .is_some_and(|pruner| !pruner.should_keep(BLOCK_NAME_COL_NAME, location_path))
            {
                continue;
            }

            // 2. prune columns by range index
            let mut columns_stat = HashMap::with_capacity(self.column_ids.len());
            let mut columns_meta = HashMap::with_capacity(self.column_ids.len());

            for column_id in &self.column_ids {
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
                continue;
            }

            // TODO(Sky): add bloom filter pruning, inverted index pruning

            let row_count = rows_count_col.index(block_idx).unwrap();
            let row_count = row_count.as_number().unwrap().as_u_int64().unwrap();
            let compression = compression_col.index(block_idx).unwrap();
            let compression =
                Compression::from_u8(*compression.as_number().unwrap().as_u_int8().unwrap());
            let create_on = create_on_col.index(block_idx).unwrap();
            let create_on = match create_on {
                ScalarRef::Null => None,
                ScalarRef::Number(number_scalar) => {
                    Some(DateTime::from_timestamp(*number_scalar.as_int64().unwrap(), 0).unwrap())
                }
                _ => unreachable!(),
            };

            let block_meta_index = BlockMetaIndex {
                segment_idx: segment_location.segment_idx,
                block_idx,
                range: None,
                page_size: *row_count as usize,
                block_id: block_id_in_segment(block_num, block_idx),
                // TODO(Sky): this is duplicate with FuseBlockPartInfo.location.
                block_location: location_path.to_string(),
                segment_location: segment_location.location.0.clone(),
                snapshot_location: segment_location.snapshot_loc.clone(),
                matched_rows: None,
                virtual_block_meta: None,
            };

            for column_id in &self.column_ids {
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
                location_path.to_string(),
                *row_count,
                columns_meta,
                Some(columns_stat),
                compression,
                None, // TODO(Sky): sort_min_max
                Some(block_meta_index),
                create_on,
            );
            self.sender
                .as_ref()
                .unwrap()
                .send(Ok(part_info))
                .await
                .unwrap();
        }
        Ok(false)
    }
}
