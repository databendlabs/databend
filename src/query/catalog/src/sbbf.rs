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

// Portions of this code are derived from Apache Arrow Parquet
// (https://github.com/apache/arrow-rs)
// Copyright Apache Software Foundation, licensed under Apache License 2.0

//! Bloom filter implementation specific to Parquet, as described
//! in the [spec][parquet-bf-spec].
//!
//! # Bloom Filter Size
//!
//! Parquet uses the [Split Block Bloom Filter][sbbf-paper] (SBBF) as its bloom filter
//! implementation. For each column upon which bloom filters are enabled, the offset and length of an SBBF
//! is stored in  the metadata for each row group in the parquet file. The size of each filter is
//! initialized using a calculation based on the desired number of distinct values (NDV) and false
//! positive probability (FPP). The FPP for a SBBF can be approximated as<sup>[1][bf-formulae]</sup>:
//!
//! ```text
//! f = (1 - e^(-k * n / m))^k
//! ```
//!
//! Where, `f` is the FPP, `k` the number of hash functions, `n` the NDV, and `m` the total number
//! of bits in the bloom filter. This can be re-arranged to determine the total number of bits
//! required to achieve a given FPP and NDV:
//!
//! ```text
//! m = -k * n / ln(1 - f^(1/k))
//! ```
//!
//! SBBFs use eight hash functions to cleanly fit in SIMD lanes<sup>[2][sbbf-paper]</sup>, therefore
//! `k` is set to 8. The SBBF will spread those `m` bits across a set of `b` blocks that
//! are each 256 bits, i.e., 32 bytes, in size. The number of blocks is chosen as:
//!
//! ```text
//! b = NP2(m/8) / 32
//! ```
//!
//! Where, `NP2` denotes *the next power of two*, and `m` is divided by 8 to be represented as bytes.
//!
//! Here is a table of calculated sizes for various FPP and NDV:
//!
//! | NDV       | FPP       | b       | Size (KB) |
//! |-----------|-----------|---------|-----------|
//! | 10,000    | 0.1       | 256     | 8         |
//! | 10,000    | 0.01      | 512     | 16        |
//! | 10,000    | 0.001     | 1,024   | 32        |
//! | 10,000    | 0.0001    | 1,024   | 32        |
//! | 100,000   | 0.1       | 4,096   | 128       |
//! | 100,000   | 0.01      | 4,096   | 128       |
//! | 100,000   | 0.001     | 8,192   | 256       |
//! | 100,000   | 0.0001    | 16,384  | 512       |
//! | 100,000   | 0.00001   | 16,384  | 512       |
//! | 1,000,000 | 0.1       | 32,768  | 1,024     |
//! | 1,000,000 | 0.01      | 65,536  | 2,048     |
//! | 1,000,000 | 0.001     | 65,536  | 2,048     |
//! | 1,000,000 | 0.0001    | 131,072 | 4,096     |
//! | 1,000,000 | 0.00001   | 131,072 | 4,096     |
//! | 1,000,000 | 0.000001  | 262,144 | 8,192     |
//!
//! [parquet-bf-spec]: https://github.com/apache/parquet-format/blob/master/BloomFilter.md
//! [sbbf-paper]: https://arxiv.org/pdf/2101.01719
//! [bf-formulae]: http://tfk.mit.edu/pdf/bloom.pdf

use core::simd::cmp::SimdPartialEq;
use core::simd::Simd;
use std::mem::size_of;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;

/// Salt values as defined in the [spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md#technical-approach).
const SALT: [u32; 8] = [
    0x47b6137b_u32,
    0x44974d91_u32,
    0x8824ad5b_u32,
    0xa2b7289d_u32,
    0x705495c7_u32,
    0x2df1424b_u32,
    0x9efc4947_u32,
    0x5c6bfb31_u32,
];

/// Each block is 256 bits, broken up into eight contiguous "words", each consisting of 32 bits.
/// Each word is thought of as an array of bits; each bit is either "set" or "not set".
#[derive(Debug, Copy, Clone)]
#[repr(transparent)]
struct Block([u32; 8]);

type U32x8 = Simd<u32, 8>;

impl Block {
    const ZERO: Block = Block([0; 8]);

    /// takes as its argument a single unsigned 32-bit integer and returns a block in which each
    /// word has exactly one bit set.
    fn mask(x: u32) -> Self {
        Self(Self::mask_simd(x).to_array())
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn to_ne_bytes(self) -> [u8; 32] {
        // SAFETY: [u32; 8] and [u8; 32] have the same size and neither has invalid bit patterns.
        unsafe { std::mem::transmute(self.0) }
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn to_le_bytes(self) -> [u8; 32] {
        self.swap_bytes().to_ne_bytes()
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn swap_bytes(mut self) -> Self {
        self.0.iter_mut().for_each(|x| *x = x.swap_bytes());
        self
    }

    /// Setting every bit in the block that was also set in the result from mask
    fn insert(&mut self, hash: u32) {
        let mask = Self::mask(hash);
        for i in 0..8 {
            self[i] |= mask[i];
        }
    }

    /// Returns true when every bit that is set in the result of mask is also set in the block.
    fn check(&self, hash: u32) -> bool {
        let mask = Self::mask_simd(hash);
        let block_vec = U32x8::from_array(self.0);
        (block_vec & mask).simd_ne(U32x8::splat(0)).all()
    }

    #[inline(always)]
    fn mask_simd(x: u32) -> U32x8 {
        let hash_vec = U32x8::splat(x);
        let salt_vec = U32x8::from_array(SALT);
        let bit_index = (hash_vec * salt_vec) >> U32x8::splat(27);
        U32x8::splat(1) << bit_index
    }
}

impl std::ops::Index<usize> for Block {
    type Output = u32;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl std::ops::IndexMut<usize> for Block {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

#[derive(Debug)]
#[repr(transparent)]
struct BlockAtomic([AtomicU32; 8]);

impl BlockAtomic {
    fn new() -> Self {
        Self(std::array::from_fn(|_| AtomicU32::new(0)))
    }

    fn insert(&self, hash: u32) {
        let mask = Block::mask(hash);
        for (slot, value) in self.0.iter().zip(mask.0.iter()) {
            slot.fetch_or(*value, Ordering::Relaxed);
        }
    }
}

/// A split block Bloom filter.
///
/// The creation of this structure is based on the [`crate::file::properties::BloomFilterProperties`]
/// struct set via [`crate::file::properties::WriterProperties`] and is thus hidden by default.
#[derive(Debug, Clone)]
pub struct Sbbf(Vec<Block>);

#[derive(Debug)]
pub struct SbbfAtomic(Vec<BlockAtomic>);

pub(crate) const BITSET_MIN_LENGTH: usize = 32;
pub(crate) const BITSET_MAX_LENGTH: usize = 128 * 1024 * 1024;

#[inline]
fn hash_to_block_index_for_blocks(hash: u64, num_blocks: usize) -> usize {
    (((hash >> 32).unchecked_mul(num_blocks as u64)) >> 32) as usize
}

#[inline]
fn optimal_num_of_bytes(num_bytes: usize) -> usize {
    let num_bytes = num_bytes.min(BITSET_MAX_LENGTH);
    let num_bytes = num_bytes.max(BITSET_MIN_LENGTH);
    num_bytes.next_power_of_two()
}

// see http://algo2.iti.kit.edu/documents/cacheefficientbloomfilters-jea.pdf
// given fpp = (1 - e^(-k * n / m)) ^ k
// we have m = - k * n / ln(1 - fpp ^ (1 / k))
// where k = number of hash functions, m = number of bits, n = number of distinct values
#[inline]
fn num_of_bits_from_ndv_fpp(ndv: u64, fpp: f64) -> usize {
    let num_bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    num_bits as usize
}

impl Sbbf {
    /// Create a new [Sbbf] with given number of distinct values and false positive probability.
    /// Will return an error if `fpp` is greater than or equal to 1.0 or less than 0.0.
    pub fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self, String> {
        if !(0.0..1.0).contains(&fpp) {
            return Err(format!(
                "False positive probability must be between 0.0 and 1.0, got {fpp}"
            ));
        }
        let num_bits = num_of_bits_from_ndv_fpp(ndv, fpp);
        Ok(Self::new_with_num_of_bytes(num_bits / 8))
    }

    /// Create a new [Sbbf] with given number of bytes, the exact number of bytes will be adjusted
    /// to the next power of two bounded by [BITSET_MIN_LENGTH] and [BITSET_MAX_LENGTH].
    pub(crate) fn new_with_num_of_bytes(num_bytes: usize) -> Self {
        let num_bytes = optimal_num_of_bytes(num_bytes);
        assert_eq!(num_bytes % size_of::<Block>(), 0);
        let num_blocks = num_bytes / size_of::<Block>();
        let bitset = vec![Block::ZERO; num_blocks];
        Self(bitset)
    }

    #[inline]
    fn hash_to_block_index(&self, hash: u64) -> usize {
        // unchecked_mul is unstable, but in reality this is safe, we'd just use saturating mul
        // but it will not saturate
        hash_to_block_index_for_blocks(hash, self.0.len())
    }

    /// Insert a hash into the filter. The caller must provide a well-distributed 64-bit hash.
    pub fn insert_hash(&mut self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        self.0[block_index].insert(hash as u32)
    }

    /// Insert a batch of hashes into the filter.
    pub fn insert_hash_batch(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            let block_index = self.hash_to_block_index(hash);
            self.0[block_index].insert(hash as u32);
        }
    }

    /// Check if a hash is in the filter. May return
    /// true for values that was never inserted ("false positive")
    /// but will always return false if a hash has not been inserted.
    pub fn check_hash(&self, hash: u64) -> bool {
        let block_index = self.hash_to_block_index(hash);
        self.0[block_index].check(hash as u32)
    }

    /// Check a batch of hashes. The callback is triggered for each matching hash index.
    pub fn check_hash_batch<F>(&self, hashes: &[u64], mut on_match: F)
    where F: FnMut(usize) {
        for (idx, &hash) in hashes.iter().enumerate() {
            let block_index = self.hash_to_block_index(hash);
            if self.0[block_index].check(hash as u32) {
                on_match(idx);
            }
        }
    }

    /// Merge another bloom filter into this one (bitwise OR operation)
    /// Panics if the filters have different sizes
    pub fn union(&mut self, other: &Self) {
        assert_eq!(
            self.0.len(),
            other.0.len(),
            "Cannot union bloom filters of different sizes"
        );

        for (self_block, other_block) in self.0.iter_mut().zip(other.0.iter()) {
            for i in 0..8 {
                self_block[i] |= other_block[i];
            }
        }
    }

    /// Return the total in memory size of this bloom filter in bytes
    pub fn estimated_memory_size(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<Block>()
    }
}

impl SbbfAtomic {
    pub fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self, String> {
        if !(0.0..1.0).contains(&fpp) {
            return Err(format!(
                "False positive probability must be between 0.0 and 1.0, got {fpp}"
            ));
        }
        let num_bits = num_of_bits_from_ndv_fpp(ndv, fpp);
        Ok(Self::new_with_num_of_bytes(num_bits / 8))
    }

    pub(crate) fn new_with_num_of_bytes(num_bytes: usize) -> Self {
        let num_bytes = optimal_num_of_bytes(num_bytes);
        assert_eq!(size_of::<BlockAtomic>(), size_of::<Block>());
        assert_eq!(num_bytes % size_of::<BlockAtomic>(), 0);
        let num_blocks = num_bytes / size_of::<BlockAtomic>();
        let bitset = (0..num_blocks).map(|_| BlockAtomic::new()).collect();
        Self(bitset)
    }

    #[inline]
    fn hash_to_block_index(&self, hash: u64) -> usize {
        hash_to_block_index_for_blocks(hash, self.0.len())
    }

    pub fn insert_hash(&self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        self.0[block_index].insert(hash as u32)
    }

    pub fn insert_hash_batch(&self, hashes: &[u64]) {
        for &hash in hashes {
            self.insert_hash(hash);
        }
    }

    pub fn insert_hash_batch_parallel(self, hashes: Vec<u64>, max_threads: usize) -> Self {
        if hashes.is_empty() || max_threads <= 1 || self.0.len() < 2 {
            self.insert_hash_batch(&hashes);
            return self;
        }

        let worker_nums = max_threads.min(hashes.len()).max(1);
        let chunk_size = hashes.len().div_ceil(worker_nums).max(1);
        let runtime = Runtime::with_worker_threads(worker_nums, Some("sbbf-insert".to_string()))
            .expect("failed to create runtime for inserting bloom filter hashes");

        let hashes = Arc::new(hashes);
        let builder = Arc::new(self);
        let total = hashes.len();
        let mut join_handlers = Vec::with_capacity(total.div_ceil(chunk_size));

        for start in (0..total).step_by(chunk_size) {
            let end = (start + chunk_size).min(total);
            let hashes = hashes.clone();
            let builder = builder.clone();

            let handler = runtime
                .try_spawn(
                    async move {
                        for hash in &hashes[start..end] {
                            builder.insert_hash(*hash);
                        }
                    },
                    None,
                )
                .expect("failed to spawn runtime task for inserting bloom filter hashes");
            join_handlers.push(handler);
        }

        runtime
            .block_on(async move {
                for handler in join_handlers {
                    handler.await?;
                }
                Ok(())
            })
            .expect("runtime bloom filter insert tasks failed");

        Arc::try_unwrap(builder)
            .expect("unexpected extra references when finishing bloom filter insert")
    }

    pub fn finish(self) -> Sbbf {
        let blocks: Vec<Block> = self
            .0
            .into_iter()
            .map(|block| {
                let mut arr = [0u32; 8];
                for (dst, src) in arr.iter_mut().zip(block.0.iter()) {
                    *dst = src.load(Ordering::Relaxed);
                }
                Block(arr)
            })
            .collect();
        Sbbf(blocks)
    }

    pub fn estimated_memory_size(&self) -> usize {
        self.0.capacity() * size_of::<BlockAtomic>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_set_quick_check() {
        for i in 0..1_000_000 {
            let result = Block::mask(i);
            assert!(result.0.iter().all(|&x| x.is_power_of_two()));
        }
    }

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000_000 {
            let mut block = Block::ZERO;
            block.insert(i);
            assert!(block.check(i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let mut sbbf = Sbbf(vec![Block::ZERO; 1_000]);
        for i in 0..1_000_000 {
            sbbf.insert_hash(i);
            assert!(sbbf.check_hash(i));
        }
    }

    #[test]
    fn test_sbbf_batch_insert_and_check() {
        let mut sbbf = Sbbf(vec![Block::ZERO; 1_000]);
        let hashes: Vec<u64> = (0..10_000).collect();
        sbbf.insert_hash_batch(&hashes);
        let mut matched = 0;
        sbbf.check_hash_batch(&hashes, |_| matched += 1);
        assert_eq!(matched, hashes.len());
    }

    #[test]
    fn test_sbbf_union() {
        let mut filter1 = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
        for i in 0..50 {
            filter1.insert_hash(i);
        }

        let mut filter2 = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
        for i in 50..100 {
            filter2.insert_hash(i);
        }

        filter1.union(&filter2);

        for i in 0..100 {
            assert!(filter1.check_hash(i));
        }
    }

    #[test]
    fn test_sbbf_atomic_parallel_matches_serial() {
        let hashes: Vec<u64> = (0..100_000)
            .map(|i| {
                let val = i as u64;
                val.wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407)
            })
            .collect();

        let mut serial = Sbbf::new_with_ndv_fpp(hashes.len() as u64, 0.01).unwrap();
        serial.insert_hash_batch(&hashes);

        let builder = SbbfAtomic::new_with_ndv_fpp(hashes.len() as u64, 0.01).unwrap();
        let builder = builder.insert_hash_batch_parallel(hashes.clone(), 8);
        let atomic = builder.finish();

        for hash in &hashes {
            assert_eq!(serial.check_hash(*hash), atomic.check_hash(*hash));
        }
    }

    #[test]
    fn test_optimal_num_of_bytes() {
        for (input, expected) in &[
            (0, 32),
            (9, 32),
            (31, 32),
            (32, 32),
            (33, 64),
            (99, 128),
            (1024, 1024),
            (999_000_000, 128 * 1024 * 1024),
        ] {
            assert_eq!(*expected, optimal_num_of_bytes(*input));
        }
    }

    #[test]
    fn test_num_of_bits_from_ndv_fpp() {
        for (fpp, ndv, num_bits) in &[
            (0.1, 10, 57),
            (0.01, 10, 96),
            (0.001, 10, 146),
            (0.1, 100, 577),
            (0.01, 100, 968),
            (0.001, 100, 1460),
            (0.1, 1000, 5772),
            (0.01, 1000, 9681),
            (0.001, 1000, 14607),
            (0.1, 10000, 57725),
            (0.01, 10000, 96815),
            (0.001, 10000, 146076),
            (0.1, 100000, 577254),
            (0.01, 100000, 968152),
            (0.001, 100000, 1460769),
            (0.1, 1000000, 5772541),
            (0.01, 1000000, 9681526),
            (0.001, 1000000, 14607697),
            (1e-50, 1_000_000_000_000, 14226231280773240832),
        ] {
            assert_eq!(*num_bits, num_of_bits_from_ndv_fpp(*ndv, *fpp) as u64);
        }
    }
}
