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

/// Doris-style compact hash table for join.
///
/// Index 0 is a sentinel (empty/chain-end). Actual rows are indexed from 1.
/// Memory per row: 4 bytes (next chain) vs current ~32 bytes (pointer-based entry).
///
/// The table is single-threaded (no atomics) — designed for per-thread use
/// under hash shuffle where each thread independently builds and probes.
/// Trait for row index types. Supports u32 (up to ~4B rows) and u64.
pub trait RowIndex:
    Copy + Default + Eq + Send + Sync + 'static + std::fmt::Debug
{
    const ZERO: Self;
    fn from_usize(v: usize) -> Self;
    fn to_usize(self) -> usize;
}

impl RowIndex for u32 {
    const ZERO: Self = 0;
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v as u32
    }
    #[inline(always)]
    fn to_usize(self) -> usize {
        self as usize
    }
}

impl RowIndex for u64 {
    const ZERO: Self = 0;
    #[inline(always)]
    fn from_usize(v: usize) -> Self {
        v as u64
    }
    #[inline(always)]
    fn to_usize(self) -> usize {
        self as usize
    }
}

/// Compact join hash table using index-based chaining.
///
/// `first[bucket]` stores the first row index in that bucket's chain.
/// `next[row_index]` stores the next row index in the same bucket's chain.
/// Chain ends when the value is `I::ZERO` (sentinel).
pub struct CompactJoinHashTable<I: RowIndex = u32> {
    /// Bucket array: first[hash & mask] = first row index (1-based)
    first: Vec<I>,
    /// Chain array: next[row_index] = next row in same bucket (0 = end)
    next: Vec<I>,
    /// Visited bitmap for right outer/semi/anti joins
    visited: Vec<u8>,
    /// Bucket count minus one, for masking
    bucket_mask: usize,
}

impl<I: RowIndex> CompactJoinHashTable<I> {
    /// Create a new compact hash table for `num_rows` rows.
    /// Bucket count is next power of 2 >= num_rows + (num_rows - 1) / 7.
    pub fn new(num_rows: usize) -> Self {
        let bucket_count = Self::calc_bucket_count(num_rows);
        CompactJoinHashTable {
            first: vec![I::ZERO; bucket_count],
            // Index 0 is sentinel, so we need num_rows + 1 entries
            next: vec![I::ZERO; num_rows + 1],
            visited: Vec::new(),
            bucket_mask: bucket_count - 1,
        }
    }

    /// Get the bucket mask for external hash computation.
    pub fn bucket_mask(&self) -> usize {
        self.bucket_mask
    }

    /// Build the hash table from precomputed bucket numbers.
    /// `bucket_nums[i]` is the bucket for row i (1-based indexing, skip index 0).
    pub fn build(&mut self, bucket_nums: &[usize]) {
        // bucket_nums[0] is unused (sentinel), actual rows start at index 1
        for (i, bucket_num) in bucket_nums.iter().enumerate().skip(1) {
            let bucket = bucket_num & self.bucket_mask;
            self.next[i] = self.first[bucket];
            self.first[bucket] = I::from_usize(i);
        }
    }

    /// Get the first row index in the given bucket.
    #[inline(always)]
    pub fn first_index(&self, bucket: usize) -> I {
        unsafe { *self.first.get_unchecked(bucket & self.bucket_mask) }
    }

    /// Get the next row index in the chain.
    #[inline(always)]
    pub fn next_index(&self, row_index: I) -> I {
        unsafe { *self.next.get_unchecked(row_index.to_usize()) }
    }

    /// Initialize visited array for right-side join types.
    pub fn init_visited(&mut self, num_rows: usize) {
        self.visited = vec![0u8; num_rows + 1];
    }

    /// Mark a row as visited.
    #[inline(always)]
    pub fn set_visited(&mut self, row_index: I) {
        unsafe {
            *self.visited.get_unchecked_mut(row_index.to_usize()) = 1;
        }
    }

    /// Check if a row has been visited.
    #[inline(always)]
    pub fn is_visited(&self, row_index: usize) -> bool {
        unsafe { *self.visited.get_unchecked(row_index) != 0 }
    }

    /// Get a reference to the visited array (for final_probe scanning).
    pub fn visited(&self) -> &[u8] {
        &self.visited
    }

    fn calc_bucket_count(num_rows: usize) -> usize {
        if num_rows == 0 {
            return 1;
        }
        // Doris formula: num_elem + (num_elem - 1) / 7, then round up to power of 2
        let target = num_rows + (num_rows.saturating_sub(1)) / 7;
        target.next_power_of_two()
    }
}

