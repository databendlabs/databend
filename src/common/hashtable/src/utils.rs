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

use std::intrinsics::assume;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::DerefMut;

use super::table0::Entry;
use super::traits::Keyable;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Hashed<K: Keyable> {
    hash: u64,
    key: K,
}

impl<K: Keyable> Hashed<K> {
    #[inline(always)]
    pub fn new(key: K) -> Self {
        Self {
            hash: K::hash(&key),
            key,
        }
    }
    #[inline(always)]
    pub fn key(self) -> K {
        self.key
    }
}

unsafe impl<K: Keyable> Keyable for Hashed<K> {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe {
            let addr = std::ptr::addr_of!((*this.as_ptr()).key);
            K::is_zero(&*(addr as *const MaybeUninit<K>))
        }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        K::equals_zero(&this.key)
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.hash
    }
}

pub struct ZeroEntry<K, V>(pub Option<Entry<K, V>>);

impl<K, V> Deref for ZeroEntry<K, V> {
    type Target = Option<Entry<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for ZeroEntry<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Drop for ZeroEntry<K, V> {
    fn drop(&mut self) {
        if let Some(e) = self.0.as_mut() {
            unsafe {
                e.val.assume_init_drop();
            }
        }
    }
}

#[inline(always)]
pub unsafe fn read_le(data: *const u8, len: usize) -> u64 {
    assume(0 < len && len <= 8);
    let s = 64 - 8 * len as isize;
    if data as usize & 2048 == 0 {
        (data as *const u64).read_unaligned() & (u64::MAX >> s)
    } else {
        (data.offset(len as isize - 8) as *const u64).read_unaligned() >> s
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
pub mod sse {
    use std::arch::x86_64::*;

    #[inline(always)]
    pub unsafe fn compare_sse2(a: *const u8, b: *const u8) -> bool {
        0xFFFF
            == _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_loadu_si128(a as *const __m128i),
                _mm_loadu_si128(b as *const __m128i),
            ))
    }

    #[inline(always)]
    pub unsafe fn compare_sse2_x4(a: *const u8, b: *const u8) -> bool {
        0xFFFF
            == _mm_movemask_epi8(_mm_and_si128(
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(a as *const __m128i),
                        _mm_loadu_si128(b as *const __m128i),
                    ),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128((a as *const __m128i).offset(1)),
                        _mm_loadu_si128((b as *const __m128i).offset(1)),
                    ),
                ),
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128((a as *const __m128i).offset(2)),
                        _mm_loadu_si128((b as *const __m128i).offset(2)),
                    ),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128((a as *const __m128i).offset(3)),
                        _mm_loadu_si128((b as *const __m128i).offset(3)),
                    ),
                ),
            ))
    }

    #[inline(always)]
    pub unsafe fn memcmp_sse(a: &[u8], b: &[u8]) -> bool {
        let mut size = a.len();
        if size != b.len() {
            return false;
        }

        let mut a = a.as_ptr();
        let mut b = b.as_ptr();

        if size <= 16 {
            if size >= 8 {
                return (a as *const u64).read_unaligned() == (b as *const u64).read_unaligned()
                    && (a.add(size - 8) as *const u64).read_unaligned()
                        == (b.add(size - 8) as *const u64).read_unaligned();
            } else if size >= 4 {
                return (a as *const u32).read_unaligned() == (b as *const u32).read_unaligned()
                    && (a.add(size - 4) as *const u32).read_unaligned()
                        == (b.add(size - 4) as *const u32).read_unaligned();
            } else if size >= 2 {
                return (a as *const u16).read_unaligned() == (b as *const u16).read_unaligned()
                    && (a.add(size - 2) as *const u16).read_unaligned()
                        == (b.add(size - 2) as *const u16).read_unaligned();
            } else if size >= 1 {
                return *a == *b;
            }
            return true;
        }

        while size >= 64 {
            if compare_sse2_x4(a, b) {
                a = a.add(64);
                b = b.add(64);

                size -= 64;
            } else {
                return false;
            }
        }

        match size / 16 {
            3 if !compare_sse2(a.add(32), b.add(32)) => false,
            2 if !compare_sse2(a.add(16), b.add(16)) => false,
            1 if !compare_sse2(a, b) => false,
            _ => compare_sse2(a.add(size - 16), b.add(size - 16)),
        }
    }
}

// for merge into:
// we use BlockInfoIndex to maintain an index for the block info in chunks.
pub struct BlockInfoIndex {
    // the intervals will be like below:
    // (0,10)(11,29),(30,38). it's ordered.
    #[allow(dead_code)]
    intervals: Vec<Interval>,
    #[allow(dead_code)]
    prefixs: Vec<u64>,
    #[allow(dead_code)]
    length: usize,
}

pub type Interval = (u32, u32);

/// the segment blocks are not sequential,because we do parallel hashtable build.
/// the block lay out in chunks could be like belows:
/// segment0_block1 |
/// segment1_block0 |  chunk0
/// segment0_block0 |
///
/// segment0_block3 |
/// segment1_block1 |  chunk1
/// segment2_block0 |
///
/// .........

impl BlockInfoIndex {
    #[allow(dead_code)]
    pub fn new_with_capacity(capacity: usize) -> Self {
        BlockInfoIndex {
            intervals: Vec::with_capacity(capacity),
            prefixs: Vec::with_capacity(capacity),
            length: 0,
        }
    }

    /// 1.interval stands for the (start,end) in chunks for one block.
    /// 2.prefix is the segment_id_block_id composition.
    /// we can promise the ordered insert from outside.
    #[allow(dead_code)]
    pub fn insert_block_offsets(&mut self, interval: Interval, prefix: u64) {
        self.intervals.push(interval);
        self.prefixs.push(prefix);
        self.length += 1;
    }

    /// we do a binary search to get the partial modified offsets
    /// we will return the Interval and prefix. For example:
    /// intervals: (0,10)(11,22),(23,40)(41,55)
    /// interval: (8,27)
    /// we will give (8,10),(23,27), we don't give the (11,12),because it's updated all.
    /// case1: |-----|------|------|
    ///            |-----------|
    /// case2: |-----|------|------|
    ///              |------|
    /// case3: |-----|------|------|
    ///                |--|
    /// case4: |-----|------|------|
    ///              |--------|           
    #[allow(dead_code)]
    pub fn get_block_info(&self, interval: Interval) -> Vec<(Interval, u64)> {
        let mut res = Vec::<(Interval, u64)>::with_capacity(2);
        let left_idx = self.search_idx(interval.0);
        let right_idx = self.search_idx(interval.1);
        let left_interval = &self.intervals[left_idx];
        let right_interval = &self.intervals[right_idx];
        // empty cases
        if left_interval.0 == interval.0 && right_interval.1 == interval.1 {
            return res;
        }
        // only one result
        if self.prefixs[left_idx] == self.prefixs[right_idx] {
            res.push(((interval.0, interval.1), self.prefixs[left_idx]));
            return res;
        }
        if left_interval.0 < interval.0 {
            res.push(((interval.0, left_interval.1), self.prefixs[left_idx]))
        }
        if right_interval.1 > interval.1 {
            res.push(((right_interval.0, interval.1), self.prefixs[right_idx]))
        }
        res
    }

    /// search idx help us to find out the intervals idx which contain offset.
    /// It must contain offset.
    #[allow(dead_code)]
    fn search_idx(&self, offset: u32) -> usize {
        let mut l = 0;
        let mut r = self.length - 1;
        while l < r {
            let mid = (l + r + 1) / 2;
            if self.intervals[mid].0 <= offset {
                l = mid;
            } else {
                r = mid - 1;
            }
        }
        l
    }
}

/// we think the build blocks count is about 1024 at most time.
impl Default for BlockInfoIndex {
    fn default() -> Self {
        Self {
            intervals: Vec::with_capacity(1024),
            prefixs: Vec::with_capacity(1024),
            length: 0,
        }
    }
}

#[test]
fn test_block_info_index() {
    // let's build [0,10][11,20][21,30],[31,40],and then find [10,37].
    // we should get [10,10],[31,37]
    let intervals: Vec<Interval> = vec![(0, 10), (11, 20), (21, 30), (31, 40)];
    let find_interval: Interval = (10, 37);
    let mut block_info_index = BlockInfoIndex::new_with_capacity(10);
    for (idx, interval) in intervals.iter().enumerate() {
        block_info_index.insert_block_offsets(*interval, idx as u64)
    }
    let result = block_info_index.get_block_info(find_interval);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, (10, 10));
    assert_eq!(result[0].1, 0);
    assert_eq!(result[1].0, (31, 37));
    assert_eq!(result[1].1, 3);

    // we find [3,7], and should get [3,7]
    let find_interval: Interval = (3, 7);
    let result = block_info_index.get_block_info(find_interval);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, (3, 7));
    assert_eq!(result[0].1, 0);

    // we find [11,20], and should get empty
    let find_interval: Interval = (11, 20);
    let result = block_info_index.get_block_info(find_interval);
    assert_eq!(result.len(), 0);

    // we find [11,20], and should get empty
    let find_interval: Interval = (11, 30);
    let result = block_info_index.get_block_info(find_interval);
    assert_eq!(result.len(), 0);

    // we find [8,13], and should get (8,10),(11,13)
    let find_interval: Interval = (8, 13);
    let result = block_info_index.get_block_info(find_interval);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, (8, 10));
    assert_eq!(result[0].1, 0);
    assert_eq!(result[1].0, (11, 13));
    assert_eq!(result[1].1, 1);

    // we find [11,23], and should get (20,23)
    let find_interval: Interval = (11, 23);
    let result = block_info_index.get_block_info(find_interval);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, (21, 23));
    assert_eq!(result[0].1, 2);
}
