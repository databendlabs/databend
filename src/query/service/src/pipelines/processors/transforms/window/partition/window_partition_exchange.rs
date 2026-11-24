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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::group_hash_entries;
use databend_common_pipeline::basic::Exchange;

use super::WindowPartitionMeta;

pub struct WindowPartitionExchange {
    hash_keys: Vec<usize>,
    num_partitions: usize,
}

impl WindowPartitionExchange {
    pub fn create(hash_keys: Vec<usize>, num_partitions: usize) -> Arc<WindowPartitionExchange> {
        Arc::new(WindowPartitionExchange {
            hash_keys,
            num_partitions,
        })
    }
}

impl Exchange for WindowPartitionExchange {
    const NAME: &'static str = "Window";
    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let num_rows = data_block.num_rows();

        // Extract the columns used for hash computation.
        let hash_cols = ProjectedBlock::project(&self.hash_keys, &data_block);

        // Compute the hash value for each row.
        let mut hashes = vec![0u64; num_rows];
        group_hash_entries(hash_cols, &mut hashes);

        // Scatter the data block to different partitions.
        let indices = hashes
            .iter()
            .map(|&hash| (hash % self.num_partitions as u64) as u32)
            .collect::<Vec<_>>();
        let scatter_blocks = DataBlock::scatter(&data_block, &indices, self.num_partitions)?;

        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        for (partition_id, data_block) in scatter_blocks.into_iter().enumerate() {
            output_data_blocks[partition_id % n].push((partition_id, data_block));
        }

        // Union data blocks for each processor.
        Ok(output_data_blocks
            .into_iter()
            .map(WindowPartitionMeta::create)
            .map(DataBlock::empty_with_meta)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use super::*;

    #[test]
    fn test_more_than_256_partitions() -> Result<()> {
        let exchange = WindowPartitionExchange::create(vec![0], 512);
        let block =
            DataBlock::new_from_columns(vec![Int32Type::from_data((0..4096).collect::<Vec<_>>())]);

        let mut outputs = exchange.partition(block, 1)?;
        assert_eq!(outputs.len(), 1);
        let meta = outputs
            .pop()
            .and_then(|block| block.get_owned_meta())
            .and_then(WindowPartitionMeta::downcast_from)
            .unwrap();

        let rows = meta
            .partitioned_data
            .iter()
            .map(|(_, block)| block.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 4096);
        assert!(
            meta.partitioned_data
                .iter()
                .any(|(id, block)| *id >= 256 && !block.is_empty())
        );
        Ok(())
    }
}
