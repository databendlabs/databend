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

use std::collections::HashSet;

use crate::ChunkIndex;
use crate::DataBlock;
use crate::DataBlockVec;

struct PartitionBlockBuilder {
    num_rows: usize,
    estimated_bytes: usize,
    data_blocks: DataBlockVec,
    indices: ChunkIndex,
}

pub struct BlockPartitionStream {
    initialize: bool,
    scatter_size: usize,
    rows_threshold: usize,
    bytes_threshold: usize,
    partitions: Vec<PartitionBlockBuilder>,
}

impl BlockPartitionStream {
    pub fn create(
        mut rows_threshold: usize,
        mut bytes_threshold: usize,
        scatter_size: usize,
    ) -> BlockPartitionStream {
        if rows_threshold == 0 {
            rows_threshold = usize::MAX;
        }

        if bytes_threshold == 0 {
            bytes_threshold = usize::MAX;
        }

        BlockPartitionStream {
            scatter_size,
            rows_threshold,
            bytes_threshold,
            initialize: false,
            partitions: vec![],
        }
    }

    pub fn partition(
        &mut self,
        indices: Vec<u64>,
        block: DataBlock,
        out_ready: bool,
    ) -> Vec<(usize, DataBlock)> {
        if block.is_empty() {
            return vec![];
        }

        if !self.initialize {
            self.initialize = true;
            self.partitions.reserve(self.scatter_size);
            for _ in 0..self.scatter_size {
                self.partitions.push(PartitionBlockBuilder {
                    num_rows: 0,
                    estimated_bytes: 0,
                    data_blocks: DataBlockVec::with_capacity(1),
                    indices: ChunkIndex::default(),
                });
            }
        }

        let block_num_rows = block.num_rows();
        let block_memory_size = block.memory_size();
        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&indices, self.scatter_size);

        for (partition_id, rows) in scatter_indices.iter().enumerate() {
            if rows.is_empty() {
                continue;
            }

            let partition = &mut self.partitions[partition_id];
            let block_id = partition.data_blocks.block_rows().len() as u32;

            partition
                .data_blocks
                .push(block.clone())
                .expect("BlockPartitionStream received blocks with mismatched schema");
            push_scatter_rows(&mut partition.indices, block_id, rows);
            partition.num_rows += rows.len();
            partition.estimated_bytes =
                partition
                    .estimated_bytes
                    .saturating_add(estimate_partition_bytes(
                        block_memory_size,
                        block_num_rows,
                        rows.len(),
                    ));
        }

        if !out_ready {
            return vec![];
        }

        let mut ready_blocks = Vec::with_capacity(self.partitions.len());
        for (id, partition) in self.partitions.iter_mut().enumerate() {
            if partition.estimated_bytes >= self.bytes_threshold
                || partition.num_rows >= self.rows_threshold
            {
                if let Some(block) = take_partition_data(partition) {
                    ready_blocks.push((id, block));
                }
            }
        }

        ready_blocks
    }

    pub fn partition_ids(&self) -> Vec<usize> {
        let mut partition_ids = vec![];

        if !self.initialize {
            return partition_ids;
        }

        for (partition_id, data) in self.partitions.iter().enumerate() {
            if data.num_rows != 0 {
                partition_ids.push(partition_id);
            }
        }
        partition_ids
    }

    pub fn take_partitions(&mut self, excluded: &HashSet<usize>) -> Vec<(usize, DataBlock)> {
        if !self.initialize {
            return vec![];
        }

        let capacity = self.partitions.len() - excluded.len();
        let mut take_blocks = Vec::with_capacity(capacity);

        for (id, partition) in self.partitions.iter_mut().enumerate() {
            if excluded.contains(&id) {
                continue;
            }

            if let Some(block) = take_partition_data(partition) {
                take_blocks.push((id, block));
            }
        }

        take_blocks
    }

    pub fn finalize_partition(&mut self, partition_id: usize) -> Option<DataBlock> {
        if !self.initialize {
            return None;
        }

        take_partition_data(&mut self.partitions[partition_id])
    }
}

fn push_scatter_rows(indices: &mut ChunkIndex, block_id: u32, rows: &[u32]) {
    let Some((&first, tail)) = rows.split_first() else {
        return;
    };

    let mut start = first;
    let mut prev = first;

    for &row in tail {
        if row == prev + 1 {
            prev = row;
            continue;
        }

        indices.push_merge_range(block_id, start, prev - start + 1);
        start = row;
        prev = row;
    }

    indices.push_merge_range(block_id, start, prev - start + 1);
}

fn estimate_partition_bytes(block_bytes: usize, total_rows: usize, selected_rows: usize) -> usize {
    debug_assert!(selected_rows <= total_rows);

    if selected_rows == 0 || block_bytes == 0 {
        return 0;
    }

    if selected_rows == total_rows {
        return block_bytes;
    }

    let estimated = (block_bytes as u128 * selected_rows as u128).div_ceil(total_rows as u128);
    estimated.min(usize::MAX as u128) as usize
}

fn take_partition_data(partition: &mut PartitionBlockBuilder) -> Option<DataBlock> {
    if partition.num_rows == 0 {
        return None;
    }

    let block = partition.data_blocks.take(&partition.indices);
    partition.data_blocks.clear();
    partition.indices.clear();
    partition.num_rows = 0;
    partition.estimated_bytes = 0;
    Some(block)
}
