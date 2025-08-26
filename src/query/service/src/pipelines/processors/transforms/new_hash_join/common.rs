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

use std::cell::SyncUnsafeCell;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::new_hash_join::JoinParams;

pub struct PartialBlock {
    avg_bytes: usize,
    block: DataBlock,
}

impl PartialBlock {
    pub fn new(block: DataBlock) -> PartialBlock {
        let avg_bytes = block.memory_size().div_ceil(block.num_rows());
        PartialBlock { avg_bytes, block }
    }
}

#[derive(Default)]
pub struct PartialBlocks {
    pub current_rows: usize,
    pub current_bytes: usize,
    pub blocks: VecDeque<PartialBlock>,
}

impl PartialBlocks {
    pub fn new() -> PartialBlocks {
        PartialBlocks {
            current_rows: 0,
            current_bytes: 0,
            blocks: VecDeque::new(),
        }
    }

    pub fn add_block(&mut self, block: DataBlock) {
        self.current_rows += block.num_rows();
        self.current_bytes += block.memory_size();
        self.blocks.push_back(PartialBlock::new(block));
    }

    pub fn compact_block(&mut self, rows: usize, bytes: usize) -> Result<DataBlock> {
        // Only split when rows exceed 2× the two-thirds point.
        if self.current_rows >= rows * 2 / 3 * 2 {
            return self.compact_block_inner(rows, bytes);
        }

        if self.current_bytes >= bytes * 2 / 3 * 2 {
            return self.compact_block_inner(rows, bytes);
        }

        let blocks = std::mem::take(&mut self.blocks)
            .into_iter()
            .map(|block| block.block)
            .collect::<Vec<_>>();

        DataBlock::concat(&blocks)
    }

    fn compact_block_inner(&mut self, rows: usize, bytes: usize) -> Result<DataBlock> {
        let mut blocks = vec![];

        let mut current_rows = 0;
        let mut current_bytes = 0;

        while let Some(mut block) = self.blocks.pop_front() {
            if current_rows + block.block.num_rows() >= rows {
                let compact_block = block.block.slice(0..rows - current_rows);
                let remain_block = block
                    .block
                    .slice(rows - current_rows..block.block.num_rows());

                blocks.push(compact_block);

                if !remain_block.is_empty() {
                    block.block = remain_block;
                    self.blocks.push_front(block);
                }

                break;
            }

            if current_bytes + block.block.memory_size() >= bytes {
                let compact_bytes = bytes - current_bytes;
                let estimated_rows = compact_bytes / block.avg_bytes;

                let compact_block = block.block.slice(0..estimated_rows);
                let remain_block = block.block.slice(estimated_rows..block.block.num_rows());

                blocks.push(compact_block);

                if !remain_block.is_empty() {
                    block.block = remain_block;
                    self.blocks.push_front(block);
                }

                break;
            }

            current_rows += block.block.num_rows();
            current_bytes += block.block.memory_size();
            blocks.push(block.block);
        }

        DataBlock::concat(&blocks)
    }
}

/// Compact small blocks into larger blocks that meet the max_rows and max_bytes requirements
pub fn compact_blocks(
    blocks: impl IntoIterator<Item = DataBlock>,
    rows: usize,
    bytes: usize,
) -> Result<Vec<DataBlock>> {
    let mut compacted_blocks = Vec::new();
    let mut current_blocks = Vec::new();
    let mut current_rows = 0;
    let mut current_bytes = 0;

    for block in blocks {
        let block_rows = block.num_rows();
        let block_bytes = block.memory_size();

        // Check if adding this block would exceed the limits
        if (current_rows + block_rows >= rows) || (current_bytes + block_bytes >= bytes) {
            // If we have accumulated blocks, compact them
            if !current_blocks.is_empty() {
                let compacted = DataBlock::concat(&current_blocks)?;
                // Free memory quickly.
                current_blocks.clear();

                if !compacted.is_empty() {
                    compacted_blocks.push(compacted);
                }

                current_rows = 0;
                current_bytes = 0;
            }

            // If the current block itself meets the requirements, add it directly
            if block_rows >= rows || block_bytes >= bytes {
                compacted_blocks.push(block);
            } else {
                // Otherwise, start a new accumulation with this block
                current_blocks.push(block);
                current_rows = block_rows;
                current_bytes = block_bytes;
            }
        } else {
            // Add block to current accumulation
            current_blocks.push(block);
            current_rows += block_rows;
            current_bytes += block_bytes;
        }
    }

    // Handle remaining blocks
    if !current_blocks.is_empty() {
        let compacted = DataBlock::concat(&current_blocks)?;
        // Free memory quickly.
        current_blocks.clear();

        if !compacted.is_empty() {
            compacted_blocks.push(compacted);
        }
    }

    Ok(compacted_blocks)
}

pub fn build_join_keys(block: DataBlock, params: &JoinParams) -> Result<DataBlock> {
    let build_keys = &params.build_keys;

    let evaluator = Evaluator::new(&block, &params.func_ctx, &BUILTIN_FUNCTIONS);
    let keys_entries: Vec<BlockEntry> = build_keys
        .iter()
        .map(|expr| {
            Ok(evaluator
                .run(expr)?
                .convert_to_full_column(expr.data_type(), block.num_rows())
                .into())
        })
        .collect::<Result<_>>()?;

    // projection data blocks
    let column_nums = block.num_columns();
    let mut block_entries = Vec::with_capacity(params.build_projections.len());

    for index in 0..column_nums {
        if !params.build_projections.contains(&index) {
            continue;
        }

        block_entries.push(block.get_by_offset(index).clone());
    }

    let mut projected_block = DataBlock::new(block_entries, block.num_rows());
    // After computing complex join key expressions, we discard unnecessary columns as soon as possible to expect the release of memory.
    drop(block);

    let is_null_equal = &params.is_null_equals;
    let mut valids = None;

    for (entry, null_equals) in keys_entries.iter().zip(is_null_equal.iter()) {
        if !null_equals {
            let (is_all_null, column_valids) = entry.as_column().unwrap().validity();

            if is_all_null {
                valids = Some(Bitmap::new_constant(false, projected_block.num_rows()));
                break;
            }

            valids = and_validities(valids, column_valids.cloned());

            if let Some(bitmap) = valids.as_ref() {
                if bitmap.null_count() == bitmap.len() {
                    break;
                }

                if bitmap.null_count() == 0 {
                    valids = None;
                }
            }
        }
    }

    for (entry, is_null) in keys_entries.into_iter().zip(is_null_equal.iter()) {
        projected_block.add_entry(match !is_null && entry.data_type().is_nullable() {
            true => entry.remove_nullable(),
            false => entry,
        });
    }

    if let Some(bitmap) = valids {
        if bitmap.null_count() != bitmap.len() {
            return projected_block.filter_with_bitmap(&bitmap);
        }
    }

    Ok(projected_block)
}

pub struct IgnorePanicMutex<T: ?Sized> {
    inner: Mutex<T>,
}

impl<T> IgnorePanicMutex<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn lock(&self) -> MutexGuard<'_, T> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A C-style cell that provides interior mutability without runtime borrow checking.
///
/// This is a thin wrapper around `SyncUnsafeCell` that allows shared mutable access
/// to the inner value without the overhead of `RefCell` or `Mutex`. It's designed
/// for performance-critical scenarios where the caller can guarantee memory safety.
///
/// # Safety
///
/// - The caller must ensure that there are no data races when accessing the inner value
/// - Multiple mutable references to the same data must not exist simultaneously
/// - This should only be used when you can statically guarantee exclusive access
///   or when protected by external synchronization mechanisms
///
/// # Use Cases
///
/// - High-performance hash join operations where contention is managed externally
/// - Single-threaded contexts where `RefCell`'s runtime checks are unnecessary overhead
/// - Data structures that implement their own synchronization protocols
pub struct CStyleCell<T: ?Sized> {
    inner: SyncUnsafeCell<T>,
}

impl<T> CStyleCell<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: SyncUnsafeCell::new(inner),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }

    /// Returns a mutable reference to the inner value.
    ///
    /// # Safety
    ///
    /// The caller must ensure that no other references (mutable or immutable)
    /// to the inner value exist when this method is called, and that the
    /// returned reference is not used concurrently with other accesses.
    #[allow(clippy::mut_from_ref)]
    pub fn as_mut(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T: ?Sized> Deref for CStyleCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}
