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
use databend_common_expression::SortColumnDescription;
use databend_common_expression::SortCompare;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_pipeline_transforms::AccumulatingTransform;

use crate::physical_plans::WindowPartitionTopNFunc;

/// A block is small when, scattered into the window partitions, each piece
/// gets fewer rows than this on average and fewer bytes than
/// `MIN_PIECE_BYTES_PER_COLUMN` per column.
const MIN_PIECE_ROWS: usize = 64;

const MIN_PIECE_BYTES_PER_COLUMN: usize = 1024;

/// Prepares the input blocks of the window exchange, which scatters each
/// block into `window_num_partitions` pieces.
///
/// A small block, such as the output of a selective filter, would be cut into
/// pieces of one or two rows. Each piece holds its own allocations per
/// column, which take more memory than its rows, and the window buffers the
/// pieces until all input arrives. So small blocks are buffered here until
/// they are not small together, and then sent as one block. Other blocks are
/// sent as they are.
///
/// With a top-n window function, the rows of each block that cannot rank
/// within `top` of their partition are dropped first.
pub struct TransformWindowPartitionBatch {
    top_n: Option<PartitionTopN>,

    min_rows: usize,
    min_bytes_per_column: usize,
    buffered_blocks: Vec<DataBlock>,
    buffered_rows: usize,
    buffered_bytes: usize,
}

impl TransformWindowPartitionBatch {
    pub fn create(num_partitions: usize) -> TransformWindowPartitionBatch {
        TransformWindowPartitionBatch {
            top_n: None,
            min_rows: num_partitions * MIN_PIECE_ROWS,
            min_bytes_per_column: num_partitions * MIN_PIECE_BYTES_PER_COLUMN,
            buffered_blocks: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
        }
    }

    pub fn with_top_n(
        mut self,
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        top: usize,
        func: WindowPartitionTopNFunc,
    ) -> TransformWindowPartitionBatch {
        self.top_n = Some(PartitionTopN::new(partition_indices, order_by, top, func));
        self
    }

    fn is_small(&self, rows: usize, bytes: usize, columns: usize) -> bool {
        rows < self.min_rows && bytes < columns * self.min_bytes_per_column
    }

    fn flush(&mut self) -> Result<DataBlock> {
        let block = DataBlock::concat(&self.buffered_blocks)?;
        self.buffered_blocks.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        Ok(block)
    }
}

impl AccumulatingTransform for TransformWindowPartitionBatch {
    const NAME: &'static str = "TransformWindowPartitionBatch";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if block.is_empty() {
            return Ok(vec![]);
        }

        let block = match &self.top_n {
            Some(top_n) => top_n.truncate(block)?,
            None => block,
        };

        let rows = block.num_rows();
        let bytes = block.memory_size();
        let columns = block.num_columns();
        // A block that is not small is sent as it is, even when small blocks
        // are buffered: concatenating it would copy all its rows. The window
        // sorts each partition later, so the order of the blocks does not
        // matter.
        if !self.is_small(rows, bytes, columns) {
            return Ok(vec![block]);
        }

        self.buffered_rows += rows;
        self.buffered_bytes += bytes;
        self.buffered_blocks.push(block);
        if self.is_small(self.buffered_rows, self.buffered_bytes, columns) {
            return Ok(vec![]);
        }
        Ok(vec![self.flush()?])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output || self.buffered_blocks.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![self.flush()?])
    }
}

struct PartitionTopN {
    partition_indices: Box<[usize]>,
    top: usize,
    func: WindowPartitionTopNFunc,
    sort_desc: Box<[SortColumnDescription]>,
}

impl PartitionTopN {
    fn new(
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        top: usize,
        func: WindowPartitionTopNFunc,
    ) -> PartitionTopN {
        assert!(top > 0);
        let sort_desc = partition_indices
            .iter()
            .map(|&offset| SortColumnDescription {
                offset,
                asc: true,
                nulls_first: false,
            })
            .chain(order_by)
            .collect::<Vec<_>>()
            .into();

        PartitionTopN {
            partition_indices: partition_indices.into_boxed_slice(),
            top,
            func,
            sort_desc,
        }
    }

    /// Keeps the rows of `block` returned by `top_n_rows`, and returns
    /// `block` itself when it keeps all of them.
    fn truncate(&self, block: DataBlock) -> Result<DataBlock> {
        let rows = self.top_n_rows(&block);
        if rows.len() == block.num_rows() {
            return Ok(block);
        }
        block.take(rows.as_slice())
    }

    /// Returns the rows of `block` that rank within `top` of their window
    /// partition among the rows of `block`. A row ranked beyond `top` inside
    /// one block is ranked beyond `top` among all rows, so dropping it does
    /// not change the window result.
    fn top_n_rows(&self, block: &DataBlock) -> Vec<u32> {
        let rows = block.num_rows();
        let mut sort_compare = SortCompare::with_force_equality(self.sort_desc.to_vec(), rows);

        for &offset in &self.partition_indices {
            let array = block.get_by_offset(offset).value();
            sort_compare.visit_value(array).unwrap();
            sort_compare.increment_column_index();
        }

        let partition_equality = sort_compare.equality_index().to_vec();

        for desc in self.sort_desc.iter().skip(self.partition_indices.len()) {
            let array = block.get_by_offset(desc.offset).value();
            sort_compare.visit_value(array).unwrap();
            sort_compare.increment_column_index();
        }

        let full_equality = sort_compare.equality_index().to_vec();
        let permutation = sort_compare.take_permutation();

        let mut kept = Vec::new();
        let mut start = 0;
        let mut cur = 0;
        while cur < rows {
            kept.push(permutation[start]);

            let mut rank = 0; // this first value is rank 0
            cur = start + 1;
            while cur < rows {
                if partition_equality[cur] == 0 {
                    start = cur;
                    break;
                }

                match self.func {
                    WindowPartitionTopNFunc::RowNumber => {
                        if cur - start < self.top {
                            kept.push(permutation[cur]);
                        }
                    }
                    WindowPartitionTopNFunc::Rank | WindowPartitionTopNFunc::DenseRank => {
                        if full_equality[cur] == 0 {
                            if matches!(self.func, WindowPartitionTopNFunc::Rank) {
                                rank = cur - start
                            } else {
                                rank += 1
                            }
                        }

                        if rank < self.top {
                            kept.push(permutation[cur]);
                        }
                    }
                }
                cur += 1;
            }
        }
        kept
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;

    use super::*;

    fn order_by_first_column() -> Vec<SortColumnDescription> {
        vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }]
    }

    fn top_n_rows(
        partition_indices: Vec<usize>,
        top: usize,
        func: WindowPartitionTopNFunc,
        block: &DataBlock,
    ) -> Vec<u32> {
        let top_n = PartitionTopN::new(partition_indices, order_by_first_column(), top, func);
        let mut rows = top_n.top_n_rows(block);
        rows.sort();
        rows
    }

    #[test]
    fn test_row_number() -> Result<()> {
        let data = DataBlock::new(
            vec![
                Int32Type::from_data(vec![3, 1, 2, 2, 4, 3, 7, 0, 3]).into(),
                BlockEntry::new_const_column_arg::<StringType>("a".to_string(), 9),
                Int32Type::from_data(vec![3, 1, 3, 2, 2, 3, 4, 3, 3]).into(),
                StringType::from_data(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"]).into(),
            ],
            9,
        );
        data.check_valid()?;

        // Partitions (b, c): (a, 1): [1], (a, 2): [3, 4], (a, 4): [6],
        // (a, 3): [7, 2, 0] out of [7, 2, 0, 5, 8].
        let rows = top_n_rows(vec![1, 2], 3, WindowPartitionTopNFunc::RowNumber, &data);
        assert_eq!(rows, vec![0, 1, 2, 3, 4, 6, 7]);
        Ok(())
    }

    #[test]
    fn test_rank() -> Result<()> {
        let data = DataBlock::new(
            vec![
                Int32Type::from_data(vec![7, 7, 7, 6, 5, 5, 4, 1, 3, 1, 1]).into(),
                Int32Type::from_data(vec![7, 6, 5, 5, 5, 4, 3, 3, 2, 3, 3]).into(),
            ],
            11,
        );
        data.check_valid()?;

        // Partition 3 keeps the three rows tied at rank 0 and drops row 6.
        let rows = top_n_rows(vec![1], 3, WindowPartitionTopNFunc::Rank, &data);
        assert_eq!(rows, vec![0, 1, 2, 3, 4, 5, 7, 8, 9, 10]);
        Ok(())
    }

    #[test]
    fn test_dense_rank() -> Result<()> {
        let data = DataBlock::new(
            vec![
                Int32Type::from_data(vec![5, 2, 3, 3, 2, 2, 1, 1, 1, 1, 1]).into(),
                Int32Type::from_data(vec![2, 2, 4, 3, 2, 2, 5, 4, 3, 3, 3]).into(),
            ],
            11,
        );
        data.check_valid()?;

        // Every row has a dense rank below 3 in its partition.
        let rows = top_n_rows(vec![1], 3, WindowPartitionTopNFunc::DenseRank, &data);
        assert_eq!(rows, (0..11).collect::<Vec<_>>());
        Ok(())
    }

    /// `rows` rows of two Int32 columns, one partition key per two rows.
    fn block_with_pairs(rows: i32) -> DataBlock {
        DataBlock::new_from_columns(vec![
            Int32Type::from_data((0..rows).collect::<Vec<_>>()),
            Int32Type::from_data((0..rows).map(|i| i / 2).collect::<Vec<_>>()),
        ])
    }

    #[test]
    fn test_small_blocks_are_batched() -> Result<()> {
        // One partition: blocks under 64 rows are small.
        let mut p = TransformWindowPartitionBatch::create(1);

        assert!(p.transform(block_with_pairs(30))?.is_empty());
        assert!(p.transform(block_with_pairs(30))?.is_empty());
        let out = p.transform(block_with_pairs(30))?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 90);
        assert!(p.buffered_blocks.is_empty());

        // The rows left at the end are sent when the input finishes.
        assert!(p.transform(block_with_pairs(10))?.is_empty());
        let out = p.on_finish(true)?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 10);
        assert!(p.on_finish(true)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_top_n_rows_are_batched() -> Result<()> {
        // `row_number` with top 1 keeps half of each block.
        let mut p = TransformWindowPartitionBatch::create(1).with_top_n(
            vec![1],
            order_by_first_column(),
            1,
            WindowPartitionTopNFunc::RowNumber,
        );

        assert!(p.transform(block_with_pairs(100))?.is_empty());
        let out = p.transform(block_with_pairs(100))?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 100);
        Ok(())
    }

    #[test]
    fn test_other_blocks_are_sent_as_they_are() -> Result<()> {
        // 64 rows is not small for one partition.
        let mut p = TransformWindowPartitionBatch::create(1);
        let data = block_with_pairs(64);
        let out = p.transform(data.clone())?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].columns(), data.columns());

        // One wide row is not small either: it has 1 KiB per column.
        let wide = DataBlock::new_from_columns(vec![StringType::from_data(vec!["x".repeat(1024)])]);
        let out = p.transform(wide.clone())?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].columns(), wide.columns());

        // A block that is not small is not concatenated with the small
        // blocks buffered before it.
        assert!(p.transform(block_with_pairs(30))?.is_empty());
        let out = p.transform(data.clone())?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].columns(), data.columns());
        assert_eq!(p.buffered_rows, 30);
        let out = p.on_finish(true)?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 30);

        // A top-n window that keeps every row sends the block as it is.
        let mut p = TransformWindowPartitionBatch::create(1).with_top_n(
            vec![1],
            order_by_first_column(),
            2,
            WindowPartitionTopNFunc::RowNumber,
        );
        let out = p.transform(data.clone())?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].columns(), data.columns());
        assert!(p.buffered_blocks.is_empty());
        Ok(())
    }
}
