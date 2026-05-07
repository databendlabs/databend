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
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipeline;

use crate::processors::AccumulatingTransform;
use crate::processors::BlockCompactMeta;
use crate::processors::TransformCompactBlock;
use crate::processors::TransformPipelineHelper;

pub fn build_ordered_compact_pipeline(
    pipeline: &mut Pipeline,
    thresholds: BlockThresholds,
    max_threads: usize,
) -> Result<()> {
    pipeline.try_resize(1)?;
    pipeline.add_accumulating_transformer(|| OrderedBlockCompactBuilder::new(thresholds));
    pipeline.try_resize(max_threads)?;
    pipeline.add_block_meta_transformer(TransformCompactBlock::default);
    Ok(())
}

pub struct OrderedBlockCompactBuilder {
    thresholds: BlockThresholds,
    // Holds blocks that exceeded the threshold and are waiting to be compacted.
    staged_blocks: BlockGroup,
    // Holds blocks that are partially accumulated but haven't reached the threshold.
    pending_blocks: BlockGroup,
}

impl OrderedBlockCompactBuilder {
    pub fn new(thresholds: BlockThresholds) -> Self {
        Self {
            thresholds,
            staged_blocks: BlockGroup::default(),
            pending_blocks: BlockGroup::default(),
        }
    }

    fn create_output_data(blocks: &mut BlockGroup, thresholds: BlockThresholds) -> DataBlock {
        // Ordered compact keeps input order first; the final materialization decides
        // whether the buffered group can stay as-is, should be concatenated, or must split.
        if thresholds.check_too_large(blocks.total_rows, blocks.total_bytes) {
            // This is a soft target used to keep post-sort blocks from growing without bound.
            // Reclustering cannot know final compressed/file sizes before serialization, so
            // this path intentionally does not enforce a hard post-sort per-block maximum.
            // A block may remain above max_rows_per_block / max_bytes_per_block when that
            // avoids creating very small tail blocks inside the compaction window.
            let block_num =
                thresholds.calc_compact_block_num(blocks.total_rows, blocks.total_bytes);
            DataBlock::empty_with_meta(Box::new(BlockCompactMeta::Split {
                blocks: blocks.take_blocks(),
                block_num,
            }))
        } else if blocks.len() > 1 {
            DataBlock::empty_with_meta(Box::new(BlockCompactMeta::Concat(blocks.take_blocks())))
        } else {
            DataBlock::empty_with_meta(Box::new(BlockCompactMeta::NoChange(blocks.take_blocks())))
        }
    }
}

impl AccumulatingTransform for OrderedBlockCompactBuilder {
    const NAME: &'static str = "OrderedBlockCompactBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let num_rows = data.num_rows();
        let num_bytes = data.estimate_block_size();

        // pending_blocks accumulates the current ordered run; once it reaches N we either
        // stage it as the next output candidate or materialize it immediately if it grows past 2N.
        let total_rows = self.pending_blocks.total_rows + num_rows;
        let total_bytes = self.pending_blocks.total_bytes + num_bytes;
        if !self.thresholds.check_large_enough(total_rows, total_bytes) {
            // blocks < N
            self.pending_blocks.push(data, num_rows, num_bytes);
            return Ok(vec![]);
        }

        let mut res = Vec::with_capacity(2);
        if !self.staged_blocks.is_empty() {
            res.push(Self::create_output_data(
                &mut self.staged_blocks,
                self.thresholds,
            ));
        }

        std::mem::swap(&mut self.staged_blocks, &mut self.pending_blocks);
        self.staged_blocks.push(data, num_rows, num_bytes);
        Ok(res)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        // Flush the final ordered run in one shot so create_output_data can still decide
        // between NoChange / Concat / Split using the aggregated size.
        self.staged_blocks.append(&mut self.pending_blocks);
        if self.staged_blocks.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![Self::create_output_data(
                &mut self.staged_blocks,
                self.thresholds,
            )])
        }
    }
}

#[derive(Default)]
struct BlockGroup {
    blocks: Vec<DataBlock>,
    total_rows: usize,
    total_bytes: usize,
}

impl BlockGroup {
    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn push(&mut self, block: DataBlock, num_rows: usize, num_bytes: usize) {
        self.total_rows += num_rows;
        self.total_bytes += num_bytes;
        self.blocks.push(block);
    }

    fn append(&mut self, other: &mut Self) {
        self.total_rows += other.total_rows;
        self.total_bytes += other.total_bytes;
        self.blocks.append(&mut other.blocks);
        other.total_rows = 0;
        other.total_bytes = 0;
    }

    fn take_blocks(&mut self) -> Vec<DataBlock> {
        self.total_rows = 0;
        self.total_bytes = 0;
        std::mem::take(&mut self.blocks)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use super::*;
    use crate::processors::BlockMetaTransform;

    fn block_with_rows(rows: usize) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(
            (0..rows).map(|v| v as i32).collect::<Vec<_>>(),
        )])
    }

    fn row_focused_thresholds() -> BlockThresholds {
        BlockThresholds::default()
            .set_rows_per_block(1000)
            .set_bytes_per_block(1 << 20)
    }

    #[test]
    fn test_ordered_compact_keeps_boundary_run_as_single_block() -> Result<()> {
        let thresholds = row_focused_thresholds();
        let mut group = BlockGroup::default();
        for rows in [300, 300, 1000] {
            let block = block_with_rows(rows);
            group.push(block.clone(), block.num_rows(), block.estimate_block_size());
        }

        let meta_block = OrderedBlockCompactBuilder::create_output_data(&mut group, thresholds);
        let meta = BlockCompactMeta::downcast_from(meta_block.get_owned_meta().unwrap()).unwrap();
        let output = TransformCompactBlock::default().transform(meta)?;

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1600);
        Ok(())
    }

    #[test]
    fn test_ordered_compact_split_allows_soft_upper_bound() -> Result<()> {
        let thresholds = row_focused_thresholds();
        let block = block_with_rows(2001);
        let mut group = BlockGroup::default();
        group.push(block.clone(), block.num_rows(), block.estimate_block_size());

        let meta_block = OrderedBlockCompactBuilder::create_output_data(&mut group, thresholds);
        let meta = BlockCompactMeta::downcast_from(meta_block.get_owned_meta().unwrap()).unwrap();
        let output = TransformCompactBlock::default().transform(meta)?;

        assert_eq!(output.len(), 2);
        assert_eq!(output[0].num_rows(), 1001);
        assert_eq!(output[1].num_rows(), 1000);
        assert!(output[0].num_rows() > thresholds.max_rows_per_block);
        Ok(())
    }

    #[test]
    fn test_ordered_compact_splits_by_target_block_count() -> Result<()> {
        let thresholds = BlockThresholds::default()
            .set_rows_per_block(5)
            .set_bytes_per_block(1 << 20);
        let mut group = BlockGroup::default();
        for rows in [2, 6, 6, 2, 2] {
            let block = block_with_rows(rows);
            group.push(block.clone(), block.num_rows(), block.estimate_block_size());
        }

        let meta_block = OrderedBlockCompactBuilder::create_output_data(&mut group, thresholds);
        let meta = BlockCompactMeta::downcast_from(meta_block.get_owned_meta().unwrap()).unwrap();
        let output = TransformCompactBlock::default().transform(meta)?;

        assert_eq!(
            output.iter().map(DataBlock::num_rows).collect::<Vec<_>>(),
            vec![6, 6, 6]
        );
        Ok(())
    }

    #[test]
    fn test_ordered_compact_split_does_not_exceed_target_block_count() -> Result<()> {
        let block = block_with_rows(1000);
        let mut group = BlockGroup::default();
        group.push(block.clone(), block.num_rows(), block.estimate_block_size());

        let target_block_count = 500;
        let thresholds = BlockThresholds::default()
            .set_rows_per_block(1000)
            .set_bytes_per_block(group.total_bytes / target_block_count);
        let block_num = thresholds.calc_compact_block_num(group.total_rows, group.total_bytes);

        let meta_block = OrderedBlockCompactBuilder::create_output_data(&mut group, thresholds);
        let meta = BlockCompactMeta::downcast_from(meta_block.get_owned_meta().unwrap()).unwrap();
        let output = TransformCompactBlock::default().transform(meta)?;

        assert!(output.len() <= block_num);
        assert!(output.iter().all(|block| block.num_rows() > 1));
        Ok(())
    }
}
