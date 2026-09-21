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

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::hint::unlikely;

use bytesize::ByteSize;
use databend_common_base::containers::FixedHeap;
use databend_common_exception::Result;
use databend_common_expression::ChunkIndex;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;

use super::core::Cursor;
use super::core::CursorOrder;
use super::core::Rows;
use super::sort_merge_base::MergeSort;

/// This is a specific version of [`super::transform_sort_merge::TransformSortMerge`] which sort blocks with limit.
pub struct TransformSortMergeLimit<R: Rows> {
    // `heap` must be dropped before `rows`, because its cursors borrow items
    // from the originating rows.
    heap: FixedHeap<Reverse<Cursor<'static, R, LocalCursorOrder>>>,
    rows: Vec<Option<R>>,
    buffer: HashMap<usize, DataBlock>,

    /// Record current memory usage.
    num_bytes: ByteSize,
    num_rows: usize,

    block_size: usize,
}

impl<R: Rows> MergeSort<R> for TransformSortMergeLimit<R> {
    const NAME: &'static str = "TransformSortMergeLimit";

    fn add_block(&mut self, block: DataBlock, init_rows: R) -> Result<()> {
        if unlikely(self.heap.cap() == 0 || block.is_empty()) {
            // limit is 0 or block is empty.
            return Ok(());
        }

        let input_index = self.rows.len();

        let block_num_bytes = block.memory_size() as u64;
        self.num_bytes += block_num_bytes;
        self.num_rows += block.num_rows();
        let cur_index = input_index;
        self.buffer.insert(cur_index, block);
        debug_assert_eq!(cur_index, self.rows.len());
        self.rows.push(Some(init_rows));

        {
            let rows = self.rows[cur_index].as_ref().unwrap();
            let num_rows = rows.len();
            debug_assert!(num_rows > 0);
            // Safety: Rows guarantees its items survive moving the Rows
            // wrapper. `self.rows` keeps the originating Rows alive while any
            // cursor for this input remains in `heap`.
            let (current, last) = unsafe { (rows.row_stable(0), rows.row_stable(num_rows - 1)) };
            let mut cursor = Cursor::new(input_index, num_rows, current, last);

            while !cursor.is_finished() {
                if let Some(Reverse(evict)) = self.heap.push(Reverse(cursor)) {
                    if evict.row_index == 0 {
                        // Evict the first row of the block,
                        // which means the block must not appear in the Top-N result.
                        if let Some(block) = self.buffer.remove(&evict.input_index) {
                            self.num_bytes -= block.memory_size() as u64;
                            self.num_rows -= block.num_rows();
                        }
                        if evict.input_index != cur_index {
                            let rows = self.rows[evict.input_index].take();
                            debug_assert!(rows.is_some());
                        }
                    }

                    if evict.input_index == cur_index {
                        // The Top-N heap is full, and later rows in current block cannot be put into the heap.
                        break;
                    }
                }
                let row_index = cursor.row_index + 1;
                // Safety: the originating Rows remains in `self.rows` for the
                // entire loop.
                let current = (row_index < num_rows).then(|| unsafe {
                    self.rows[cur_index].as_ref().unwrap().row_stable(row_index)
                });
                cursor.advance(1, current);
            }
        }

        if !self.buffer.contains_key(&cur_index) {
            let rows = self.rows[cur_index].take();
            debug_assert!(rows.is_some());
        }

        // String views may keep source buffers alive after filtering or slicing. Compact only
        // blocks that remain in the Top-N candidate set so discarded blocks avoid the copy.
        if let Some(block) = self.buffer.remove(&cur_index) {
            self.num_bytes -= block_num_bytes;
            let block = block.maybe_gc();
            self.num_bytes += block.memory_size() as u64;
            self.buffer.insert(cur_index, block);
        }

        Ok(())
    }

    fn on_finish(&mut self, all_in_one_block: bool) -> Result<Vec<DataBlock>> {
        if all_in_one_block {
            Ok(self.drain_heap(self.num_rows))
        } else {
            Ok(self.drain_heap(self.block_size))
        }
    }

    #[inline(always)]
    fn num_bytes(&self) -> ByteSize {
        self.num_bytes
    }

    #[inline(always)]
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn prepare_spill(&mut self, spill_batch_size: usize) -> Result<Vec<DataBlock>> {
        // TBD: if it's better to add the blocks back to the heap.
        // Reason: the output `blocks` is a result of Top-N,
        // so the memory usage will be less than the original buffered data.
        // If the reduced memory usage does not reach the spilling threshold,
        // we can avoid one spilling.
        let blocks = self.drain_heap(spill_batch_size);

        debug_assert!(self.buffer.is_empty());

        Ok(blocks)
    }
}

#[derive(Clone, Copy)]
struct LocalCursorOrder;

impl<R: Rows> CursorOrder<R> for LocalCursorOrder {
    fn eq<'a>(a: &Cursor<'a, R, Self>, b: &Cursor<'a, R, Self>) -> bool {
        (a.input_index == b.input_index && a.row_index == b.row_index) || a.current() == b.current()
    }

    fn cmp<'a>(a: &Cursor<'a, R, Self>, b: &Cursor<'a, R, Self>) -> Ordering {
        if a.input_index == b.input_index {
            return a.row_index.cmp(&b.row_index);
        }
        a.current()
            .cmp(&b.current())
            .then_with(|| a.input_index.cmp(&b.input_index))
    }
}

impl<R: Rows> TransformSortMergeLimit<R> {
    pub fn create(block_size: usize, limit: usize) -> Self {
        TransformSortMergeLimit {
            heap: FixedHeap::new(limit),
            rows: Vec::with_capacity(limit),
            buffer: HashMap::with_capacity(limit),
            block_size,
            num_bytes: ByteSize(0),
            num_rows: 0,
        }
    }

    fn drain_heap(&mut self, batch_size: usize) -> Vec<DataBlock> {
        if self.heap.is_empty() {
            return vec![];
        }

        let mut blocks = DataBlockVec::with_capacity(self.buffer.len());
        for block in self.buffer.values().cloned() {
            blocks.push(block).unwrap();
        }

        let mut output_blocks = Vec::with_capacity(self.heap.len().div_ceil(batch_size));
        let mut output_indices = ChunkIndex::default();
        let block_indices = self.buffer.keys().cloned().collect::<Vec<_>>();
        while let Some(Reverse(cursor)) = self.heap.pop() {
            let block_index = block_indices
                .iter()
                .position(|i| *i == cursor.input_index)
                .unwrap();
            output_indices.push_merge(block_index as _, cursor.row_index as _);

            if output_indices.num_rows() >= batch_size {
                output_blocks.push(blocks.take(&output_indices));
                output_indices.clear();
            }
        }
        if output_indices.num_rows() > 0 {
            output_blocks.push(blocks.take(&output_indices));
        }

        self.buffer.clear();
        self.rows.clear();
        self.num_bytes = ByteSize(0);
        self.num_rows = 0;

        output_blocks
    }
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;

    use super::MergeSort;
    use super::TransformSortMergeLimit;
    use crate::sorts::core::Rows;
    use crate::sorts::core::SimpleRowsAsc;
    use crate::sorts::core::VariableRows;

    #[test]
    fn test_top_n_core_cursor_rows_lifetime() -> Result<()> {
        let mut sort = TransformSortMergeLimit::<VariableRows>::create(4_096, 3);
        for (keys, values) in [
            (
                vec![b"10".as_slice(), b"20".as_slice(), b"30".as_slice()],
                vec![10, 20, 30],
            ),
            (
                vec![b"01".as_slice(), b"02".as_slice(), b"03".as_slice()],
                vec![1, 2, 3],
            ),
        ] {
            let block = DataBlock::new_from_columns(vec![Int32Type::from_data(values)]);
            let rows = VariableRows::from_column(&BinaryType::from_data(keys))?;
            sort.add_block(block, rows)?;
        }

        assert_eq!(sort.rows.iter().flatten().count(), 1);
        let output = sort.on_finish(true)?;
        assert!(sort.rows.is_empty());
        assert_eq!(output.len(), 1);
        let values = Int32Type::try_downcast_column(&output[0].get_by_offset(0).to_column())?;
        assert_eq!(values.as_slice(), &[1, 2, 3]);

        Ok(())
    }

    #[test]
    fn test_top_n_rows_vec_reuses_indices_after_drain() -> Result<()> {
        let mut sort = TransformSortMergeLimit::<SimpleRowsAsc<Int32Type>>::create(4_096, 2);

        for values in [vec![2, 3], vec![0, 1]] {
            let column = Int32Type::from_data(values);
            let block = DataBlock::new_from_columns(vec![column.clone()]);
            let rows = SimpleRowsAsc::<Int32Type>::from_column(&column)?;
            sort.add_block(block, rows)?;
        }
        assert_eq!(sort.rows.len(), 2);

        let output = sort.on_finish(true)?;
        assert!(!output.is_empty());
        assert!(sort.rows.is_empty());

        let column = Int32Type::from_data(vec![4, 5]);
        let block = DataBlock::new_from_columns(vec![column.clone()]);
        let rows = SimpleRowsAsc::<Int32Type>::from_column(&column)?;
        sort.add_block(block, rows)?;
        assert_eq!(sort.rows.len(), 1);

        Ok(())
    }

    #[test]
    fn test_top_n_compacts_retained_string_views() -> Result<()> {
        const SOURCE_ROWS: i32 = 2_000;
        const LIMIT: usize = 10;

        let payload_suffix = "x".repeat(256);
        let keys = (0..SOURCE_ROWS).collect::<Vec<_>>();
        let payloads = keys
            .iter()
            .map(|key| format!("{key:08}-{payload_suffix}"))
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(keys),
            StringType::from_data(payloads),
        ])
        .slice(0..LIMIT);
        let rows = SimpleRowsAsc::<Int32Type>::from_column(&block.get_by_offset(0).to_column())?;

        let mut sort = TransformSortMergeLimit::create(4_096, LIMIT);
        sort.add_block(block, rows)?;

        let retained = sort.buffer.values().next().unwrap();
        let Column::String(payloads) = retained.get_by_offset(1).to_column() else {
            unreachable!("expected string payload column")
        };
        assert_eq!(payloads.total_bytes_len(), LIMIT * (8 + 1 + 256));
        assert!(
            payloads.total_buffer_len() < 16 * 1024,
            "Top-N retained {} bytes of source string buffers",
            payloads.total_buffer_len(),
        );
        assert_eq!(sort.num_bytes().0, retained.memory_size() as u64);

        Ok(())
    }
}
