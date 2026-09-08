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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

pub struct PartitionedBlock {
    pub partition_id: usize,
    pub block: DataBlock,
}

impl PartitionedBlock {
    pub fn create(partition_id: usize, block: DataBlock) -> Self {
        Self {
            partition_id,
            block,
        }
    }
}

pub trait PartitionStream: Send {
    fn push(&mut self, data_block: DataBlock) -> Result<Vec<PartitionedBlock>>;

    fn finish(&mut self) -> Result<Vec<PartitionedBlock>> {
        Ok(vec![])
    }
}

pub fn pre_partitioned_blocks(
    blocks: Vec<DataBlock>,
    partitions: usize,
) -> Result<Vec<PartitionedBlock>> {
    if blocks.len() != partitions {
        return Err(databend_common_exception::ErrorCode::Internal(format!(
            "Partition stream returned {} blocks for {partitions} partitions",
            blocks.len()
        )));
    }

    Ok(blocks
        .into_iter()
        .enumerate()
        .map(|(partition_id, block)| PartitionedBlock::create(partition_id, block))
        .collect())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;

    use super::pre_partitioned_blocks;

    #[test]
    fn test_pre_partitioned_blocks_validates_and_assigns_partition_ids() {
        let blocks = pre_partitioned_blocks(vec![DataBlock::empty(), DataBlock::empty()], 2)
            .expect("matching partition count");
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].partition_id, 0);
        assert_eq!(blocks[1].partition_id, 1);

        assert!(pre_partitioned_blocks(vec![DataBlock::empty()], 2).is_err());
    }
}
