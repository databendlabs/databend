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

use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;

/// Accumulates rows from input blocks into fixed-size output chunks
/// using mutable ColumnBuilders. When the accumulated rows reach
/// `chunk_size`, a chunk is flushed and returned.
pub struct FixedSizeChunkAccumulator {
    chunk_size: usize,
    builder_rows: usize,
    builders: Vec<ColumnBuilder>,
}

impl FixedSizeChunkAccumulator {
    pub fn new(chunk_size: usize) -> Self {
        FixedSizeChunkAccumulator {
            chunk_size,
            builders: vec![],
            builder_rows: 0,
        }
    }

    pub fn accumulate(&mut self, block: DataBlock) -> Vec<DataBlock> {
        let mut output = Vec::new();
        self.append_block(block, &mut output);
        output
    }

    pub fn finalize(&mut self) -> Option<DataBlock> {
        match self.builder_rows {
            0 => None,
            _ => Some(self.build_chunk()),
        }
    }

    fn ensure_builders(&mut self, block: &DataBlock) {
        if self.builders.is_empty() {
            self.builders = block
                .columns()
                .iter()
                .map(|entry| ColumnBuilder::with_capacity(&entry.data_type(), self.chunk_size))
                .collect();
        }
    }

    fn append_block(&mut self, block: DataBlock, output: &mut Vec<DataBlock>) {
        self.ensure_builders(&block);

        let block_rows = block.num_rows();
        let columns: Vec<Column> = block
            .take_columns()
            .into_iter()
            .map(|e| e.to_column())
            .collect();

        let mut offset = 0;
        while offset < block_rows {
            let remaining_capacity = self.chunk_size - self.builder_rows;
            let rows_to_copy = (block_rows - offset).min(remaining_capacity);

            for (builder, col) in self.builders.iter_mut().zip(columns.iter()) {
                let sliced = col.slice(offset..offset + rows_to_copy);
                builder.append_column(&sliced);
            }

            self.builder_rows += rows_to_copy;
            offset += rows_to_copy;

            if self.builder_rows == self.chunk_size {
                output.push(self.build_chunk());
            }
        }
    }

    fn build_chunk(&mut self) -> DataBlock {
        let num_rows = self.builder_rows;

        let builders = std::mem::take(&mut self.builders);

        // Reinitialize builders with same column types for next chunk.
        let mut new_builders = Vec::with_capacity(builders.len());
        let mut columns = Vec::with_capacity(builders.len());
        for b in builders {
            let dt = b.data_type();
            columns.push(BlockEntry::from(b.build()));
            new_builders.push(ColumnBuilder::with_capacity(&dt, self.chunk_size));
        }

        self.builder_rows = 0;
        self.builders = new_builders;

        DataBlock::new(columns, num_rows)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;

    use super::*;

    fn make_int_block(values: Vec<i32>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(values)])
    }

    fn extract_int_col(block: &DataBlock) -> Vec<i32> {
        let col = block.get_by_offset(0).to_column();
        let col = Int32Type::try_downcast_column(&col).unwrap();
        col.iter().copied().collect()
    }

    #[test]
    fn test_single_block_under_chunk_size() {
        let mut acc = FixedSizeChunkAccumulator::new(4);
        let chunks = acc.accumulate(make_int_block(vec![1, 2, 3]));
        assert!(chunks.is_empty());

        let last = acc.finalize().unwrap();
        assert_eq!(extract_int_col(&last), vec![1, 2, 3]);
    }

    #[test]
    fn test_exact_chunk_size() {
        let mut acc = FixedSizeChunkAccumulator::new(3);
        let chunks = acc.accumulate(make_int_block(vec![1, 2, 3]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(extract_int_col(&chunks[0]), vec![1, 2, 3]);

        assert!(acc.finalize().is_none());
    }

    #[test]
    fn test_block_larger_than_chunk_size() {
        let mut acc = FixedSizeChunkAccumulator::new(3);
        let chunks = acc.accumulate(make_int_block(vec![1, 2, 3, 4, 5, 6, 7]));
        assert_eq!(chunks.len(), 2);
        assert_eq!(extract_int_col(&chunks[0]), vec![1, 2, 3]);
        assert_eq!(extract_int_col(&chunks[1]), vec![4, 5, 6]);

        let last = acc.finalize().unwrap();
        assert_eq!(extract_int_col(&last), vec![7]);
    }

    #[test]
    fn test_multiple_small_blocks() {
        let mut acc = FixedSizeChunkAccumulator::new(4);
        assert!(acc.accumulate(make_int_block(vec![1, 2])).is_empty());
        let chunks = acc.accumulate(make_int_block(vec![3, 4, 5]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(extract_int_col(&chunks[0]), vec![1, 2, 3, 4]);

        let last = acc.finalize().unwrap();
        assert_eq!(extract_int_col(&last), vec![5]);
    }

    #[test]
    fn test_flush_empty() {
        let mut acc = FixedSizeChunkAccumulator::new(4);
        assert!(acc.finalize().is_none());
    }

    #[test]
    fn test_multi_column_blocks() {
        let mut acc = FixedSizeChunkAccumulator::new(3);
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3, 4, 5]),
            StringType::from_data(vec!["a", "b", "c", "d", "e"]),
        ]);
        let chunks = acc.accumulate(block);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 3);
        assert_eq!(chunks[0].num_columns(), 2);

        let last = acc.finalize().unwrap();
        assert_eq!(last.num_rows(), 2);
        assert_eq!(last.num_columns(), 2);

        let int_col = Int32Type::try_downcast_column(&last.get_by_offset(0).to_column()).unwrap();
        let str_col = StringType::try_downcast_column(&last.get_by_offset(1).to_column()).unwrap();
        assert_eq!(int_col.iter().copied().collect::<Vec<_>>(), vec![4, 5]);
        let strs: Vec<&str> = str_col.iter().collect();
        assert_eq!(strs, vec!["d", "e"]);
    }

    #[test]
    fn test_reuse_after_flush() {
        let mut acc = FixedSizeChunkAccumulator::new(2);
        let chunks = acc.accumulate(make_int_block(vec![1, 2]));
        assert_eq!(chunks.len(), 1);
        assert!(acc.finalize().is_none());

        // Accumulator can be reused after flush
        let chunks = acc.accumulate(make_int_block(vec![3, 4, 5]));
        assert_eq!(chunks.len(), 1);
        assert_eq!(extract_int_col(&chunks[0]), vec![3, 4]);

        let last = acc.finalize().unwrap();
        assert_eq!(extract_int_col(&last), vec![5]);
    }
}
