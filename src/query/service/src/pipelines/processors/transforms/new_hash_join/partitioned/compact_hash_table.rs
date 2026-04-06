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

use databend_common_column::bitmap::Bitmap;

/// Index 0 is a sentinel (empty/chain-end). Actual rows are indexed from 1.
/// Memory per row: 4 bytes (next chain) vs current ~32 bytes (pointer-based entry).
///
/// The table is single-threaded (no atomics) — designed for per-thread use
/// under hash shuffle where each thread independently builds and probes.
/// Trait for row index types. Supports u32 (up to ~4B rows) and u64.
pub trait RowIndex: Copy + Default + Eq + Send + Sync + 'static + std::fmt::Debug {
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
    pub next: Vec<I>,
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
            bucket_mask: bucket_count - 1,
        }
    }

    /// Create a direct-mapping hash table where keys are used as array indices.
    /// `range` is `max_key - min_key`; the caller subtracts min_key before insertion/probe.
    pub fn new_direct(num_rows: usize, range: usize) -> Self {
        CompactJoinHashTable {
            first: vec![I::ZERO; range + 1],
            next: vec![I::ZERO; num_rows + 1],
            bucket_mask: 0,
        }
    }

    pub fn insert_chunk<const DIRECT: bool>(&mut self, vals: &[u64], row_offset: usize) {
        for (i, v) in vals.iter().enumerate() {
            let row_index = row_offset + i;
            let bucket = match DIRECT {
                true => *v as usize,
                false => (*v as usize) & self.bucket_mask,
            };

            self.next[row_index] = self.first[bucket];
            self.first[bucket] = I::from_usize(row_index);
        }
    }

    fn calc_bucket_count(num_rows: usize) -> usize {
        if num_rows == 0 {
            return 1;
        }

        let target = num_rows + (num_rows.saturating_sub(1)) / 7;
        target.next_power_of_two()
    }

    pub fn log_stats(&self) {
        let num_buckets = self.first.len();
        let mut non_empty = 0usize;
        let mut max_chain = 0usize;
        let mut total_chain = 0usize;

        for i in 0..num_buckets {
            if self.first[i] != I::ZERO {
                non_empty += 1;
                let mut chain_len = 0usize;
                let mut idx = self.first[i].to_usize();
                while idx != 0 {
                    chain_len += 1;
                    idx = self.next[idx].to_usize();
                }
                max_chain = max_chain.max(chain_len);
                total_chain += chain_len;
            }
        }

        let avg_chain = if non_empty > 0 {
            total_chain as f64 / non_empty as f64
        } else {
            0.0
        };
        let occupancy = if num_buckets > 0 {
            non_empty as f64 / num_buckets as f64 * 100.0
        } else {
            0.0
        };

        log::info!(
            "CompactJoinHashTable stats: buckets={}, non_empty={}, occupancy={:.1}%, total_rows={}, avg_chain={:.2}, max_chain={}",
            num_buckets, non_empty, occupancy, total_chain, avg_chain, max_chain
        );
    }

    pub fn probe<const DIRECT: bool>(&self, vals: &mut [u64], bitmap: Option<Bitmap>) -> usize {
        let mut valids = None;

        if let Some(bitmap) = bitmap {
            if bitmap.null_count() == bitmap.len() {
                vals.iter_mut().for_each(|v| {
                    *v = 0;
                });
                return 0;
            } else if bitmap.null_count() > 0 {
                valids = Some(bitmap);
            }
        }

        let mut count = 0;
        let first_len = self.first.len();

        match valids {
            Some(valids) => {
                for (valid, val) in valids.iter().zip(vals.iter_mut()) {
                    if valid {
                        let bucket = match DIRECT {
                            false => (*val as usize) & self.bucket_mask,
                            true if (*val as usize) < first_len => *val as usize,
                            true => {
                                *val = 0;
                                continue;
                            }
                        };

                        if self.first[bucket] != I::default() {
                            *val = self.first[bucket].to_usize() as u64;
                            count += 1;
                        } else {
                            *val = 0;
                        }
                    } else {
                        *val = 0;
                    }
                }
            }
            None => {
                vals.iter_mut().for_each(|val| {
                    let bucket = if DIRECT {
                        let b = *val as usize;
                        if b >= first_len {
                            *val = 0;
                            return;
                        }
                        b
                    } else {
                        (*val as usize) & self.bucket_mask
                    };
                    if self.first[bucket] != I::default() {
                        *val = self.first[bucket].to_usize() as u64;
                        count += 1;
                    } else {
                        *val = 0;
                    }
                });
            }
        }
        count
    }
}
