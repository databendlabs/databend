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
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::BlockReadInfo;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnMetaV0;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Statistics;

use crate::operations::common::BlockMetaIndex;
use crate::operations::mutation::CompactBlockPartInfo;
use crate::operations::CompactExtraInfo;
use crate::operations::CompactTaskInfo;
use crate::statistics::reducers::merge_statistics_mut;

#[async_trait::async_trait]
pub trait CompactTaskBuilder: Send {
    type Segment: AbstractSegment;
    async fn build_tasks(
        &mut self,
        segment_indices: Vec<usize>,
        compact_segments: Vec<Arc<Self::Segment>>,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<PartInfoPtr>>;
    fn new(
        column_ids: HashSet<ColumnId>,
        cluster_key_id: Option<u32>,
        thresholds: BlockThresholds,
    ) -> Self;
}

pub struct ColumnOrientedCompactTaskBuilder {
    cluster_key_id: Option<u32>,
    column_ids: HashSet<ColumnId>,
    thresholds: BlockThresholds,

    blocks: Vec<Arc<BlockReadInfo>>,
    total_rows: usize,
    total_size: usize,
}

#[async_trait::async_trait]
impl CompactTaskBuilder for ColumnOrientedCompactTaskBuilder {
    type Segment = ColumnOrientedSegment;

    async fn build_tasks(
        &mut self,
        segment_indices: Vec<usize>,
        compact_segments: Vec<Arc<Self::Segment>>,
        _semaphore: Arc<Semaphore>,
    ) -> Result<Vec<PartInfoPtr>> {
        let mut tasks = Vec::new();
        let mut removed_segment_indexes = segment_indices;
        let segment_idx = removed_segment_indexes.pop().unwrap();
        let mut block_idx = 0;
        let removed_segment_summary =
            compact_segments
                .iter()
                .fold(Statistics::default(), |mut summary, segment| {
                    merge_statistics_mut(&mut summary, segment.summary(), self.cluster_key_id);
                    summary
                });

        for segment in compact_segments.iter().rev() {
            let row_count_col = segment.row_count_col();
            let block_size_col = segment.block_size_col();
            let compression_col = segment.compression_col();
            let location_col = segment.location_path_col();
            let col_meta_cols = segment.col_meta_cols(&self.column_ids);

            for i in 0..row_count_col.len() {
                let total_rows = self.total_rows + row_count_col[i] as usize;
                let total_size = self.total_size + block_size_col[i] as usize;
                let mut col_metas = HashMap::new();
                for column_id in &self.column_ids {
                    let Some(meta_col) = col_meta_cols.get(column_id) else {
                        continue;
                    };
                    let meta = meta_col.index(i).unwrap();
                    let meta = meta.as_tuple().unwrap();
                    let offset = meta[0].as_number().unwrap().as_u_int64().unwrap();
                    let length = meta[1].as_number().unwrap().as_u_int64().unwrap();
                    let num_values = meta[2].as_number().unwrap().as_u_int64().unwrap();
                    col_metas.insert(
                        *column_id,
                        ColumnMeta::Parquet(ColumnMetaV0 {
                            offset: *offset,
                            len: *length,
                            num_values: *num_values,
                        }),
                    );
                }
                let block = Arc::new(BlockReadInfo {
                    row_count: row_count_col[i],
                    block_size: block_size_col[i],
                    compression: Compression::from_u8(compression_col[i]),
                    location: location_col.index(i).unwrap().to_string(),
                    col_metas,
                });
                if !self.thresholds.check_large_enough(total_rows, total_size) {
                    // blocks < N
                    self.blocks.push(block);
                    self.total_rows = total_rows;
                    self.total_size = total_size;
                } else if self.thresholds.check_for_compact(total_rows, total_size) {
                    // N <= blocks < 2N
                    self.blocks.push(block);
                    let blocks = self.take_blocks();
                    tasks.push((block_idx, blocks));
                    block_idx += 1;
                } else {
                    todo!()
                }
            }
        }
        let mut blocks = self.take_blocks();
        if !blocks.is_empty() {
            let last_task = tasks.pop().map_or(vec![], |(_, v)| v);
            let total_rows = blocks.iter().map(|x| x.row_count as usize).sum::<usize>()
                + last_task
                    .iter()
                    .map(|x| x.row_count as usize)
                    .sum::<usize>();
            let total_size = blocks.iter().map(|x| x.block_size as usize).sum::<usize>()
                + last_task
                    .iter()
                    .map(|x| x.block_size as usize)
                    .sum::<usize>();

            if self.thresholds.check_for_compact(total_rows, total_size) {
                blocks.extend(last_task);
                tasks.push((block_idx, blocks));
            } else {
                // blocks >= 2N
                tasks.push((block_idx, blocks));
                tasks.push((block_idx + 1, last_task));
            }
        }

        let mut partitions: Vec<PartInfoPtr> = Vec::with_capacity(tasks.len() + 1);
        for (block_idx, blocks) in tasks.into_iter() {
            partitions.push(Arc::new(Box::new(CompactBlockPartInfo::CompactTaskInfo(
                CompactTaskInfo::create(blocks, BlockMetaIndex {
                    segment_idx,
                    block_idx,
                }),
            ))));
        }

        partitions.push(Arc::new(Box::new(CompactBlockPartInfo::CompactExtraInfo(
            CompactExtraInfo::create(
                segment_idx,
                vec![],
                removed_segment_indexes,
                removed_segment_summary,
            ),
        ))));
        Ok(partitions)
    }

    fn new(
        column_ids: HashSet<ColumnId>,
        cluster_key_id: Option<u32>,
        thresholds: BlockThresholds,
    ) -> Self {
        Self {
            column_ids,
            cluster_key_id,
            thresholds,
            blocks: vec![],
            total_rows: 0,
            total_size: 0,
        }
    }
}

impl ColumnOrientedCompactTaskBuilder {
    fn take_blocks(&mut self) -> Vec<Arc<BlockReadInfo>> {
        self.total_rows = 0;
        self.total_size = 0;
        std::mem::take(&mut self.blocks)
    }
}
