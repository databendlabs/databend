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

use databend_common_base::runtime::drop_guard;

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
        drop_guard(move || {
            if let Some(e) = self.0.as_mut() {
                unsafe {
                    e.val.assume_init_drop();
                }
            }
        })
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
#[inline]
pub fn fast_memcmp(a: &[u8], b: &[u8]) -> bool {
    unsafe { sse::memcmp_sse(a, b) }
}

#[cfg(not(all(any(target_arch = "x86_64"), target_feature = "sse4.2")))]
#[inline]
pub fn fast_memcmp(a: &[u8], b: &[u8]) -> bool {
    a == b
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

    /// # Safety
    /// This is safe that we compare bytes via addr
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
            3 if !compare_sse2(a.add(32), b.add(32))
                || !compare_sse2(a.add(16), b.add(16))
                || !compare_sse2(a, b) =>
            {
                false
            }
            2 if !compare_sse2(a.add(16), b.add(16)) || !compare_sse2(a, b) => false,
            1 if !compare_sse2(a, b) => false,
            _ => compare_sse2(a.add(size).sub(16), b.add(size).sub(16)),
        }
    }
}

// This Index is only used for target build merge into (both standalone and distributed mode).
// Advantages:
//      1. Reduces redundant I/O operations, enhancing performance.
//      2. Lowers the maintenance overhead of deduplicating row_id.(But in distributed design, we also need to give rowid)
//      3. Allows the scheduling of the subsequent mutation pipeline to be entirely allocated to not matched append operations.
// Disadvantages:
//      1. This solution is likely to be a one-time approach (especially if there are not matched insert operations involved),
// potentially leading to the target table being unsuitable for use as a build table in the future.
//      2. Requires a significant amount of memory to be efficient and currently does not support spill operations.
// for now we just support sql like below:
// `merge into t using source on xxx when matched then update xxx when not matched then insert xxx.
// for merge into:
// we use MergeIntoBlockInfoIndex to maintain an index for the block info in chunks.

pub struct MergeIntoBlockInfoIndex {
    // the intervals will be like below:
    // (0,10)(11,29),(30,38). it's ordered.
    pub intervals: Vec<Interval>,
    prefixs: Vec<u64>,
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
impl MergeIntoBlockInfoIndex {
    pub fn new_with_capacity(capacity: usize) -> Self {
        MergeIntoBlockInfoIndex {
            intervals: Vec::with_capacity(capacity),
            prefixs: Vec::with_capacity(capacity),
            length: 0,
        }
    }

    /// 1.interval stands for the (start,end) in chunks for one block.
    /// 2.prefix is the segment_id_block_id composition.
    /// we can promise the ordered insert from outside.
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
    fn get_block_info(&self, interval: Interval) -> Vec<(Interval, u64)> {
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

    pub fn gather_matched_all_blocks(&self, hits: &[u8]) -> Vec<u64> {
        let mut res = Vec::with_capacity(10);
        let mut step = 0;
        while step < hits.len() {
            if hits[step] == 1 {
                break;
            }
            step += 1;
        }
        if step == hits.len() {
            return res;
        }
        let mut start = step;
        let mut end = step;
        while start < hits.len() {
            while end < hits.len() && hits[end] == 1 {
                end += 1;
            }
            let left = self.search_idx(start as u32);
            let right = self.search_idx((end - 1) as u32);
            if left == right {
                // matched only one block.
                if self.intervals[left].0 == (start as u32)
                    && self.intervals[right].1 == (end - 1) as u32
                {
                    res.push(self.prefixs[left]);
                }
            } else {
                assert!(right > left);
                // 1. left most side.
                if self.intervals[left].0 == start as u32 {
                    res.push(self.prefixs[left]);
                }
                for idx in left + 1..right {
                    res.push(self.prefixs[idx]);
                }
                // 2. right most side.
                if self.intervals[right].1 == (end - 1) as u32 {
                    res.push(self.prefixs[right]);
                }
            }
            while end < hits.len() && hits[end] == 0 {
                end += 1;
            }
            start = end;
        }
        res
    }

    pub fn gather_all_partial_block_offsets(&self, hits: &[u8]) -> Vec<(Interval, u64)> {
        let mut res = Vec::with_capacity(10);
        let mut step = 0;
        while step < hits.len() {
            if hits[step] == 0 {
                break;
            }
            step += 1;
        }
        if step == hits.len() {
            return res;
        }
        let mut start = step;
        let mut end = step;
        while start < hits.len() {
            while end < hits.len() && hits[end] == 0 {
                end += 1;
            }
            res.extend(self.get_block_info((start as u32, (end - 1) as u32)));
            while end < hits.len() && hits[end] == 1 {
                end += 1;
            }
            start = end;
        }
        res
    }

    /// return [{(Interval,prefix),(Interval,prefix)},chunk_idx]
    pub fn chunk_offsets(
        &self,
        partial_unmodified: &Vec<(Interval, u64)>,
        chunks_offsets: &Vec<u32>,
    ) -> Vec<(Vec<(Interval, u64)>, u64)> {
        let mut res = Vec::with_capacity(chunks_offsets.len());
        if chunks_offsets.is_empty() {
            assert!(partial_unmodified.is_empty());
        }
        if partial_unmodified.is_empty() || chunks_offsets.is_empty() {
            return res;
        }
        let mut chunk_idx = 0;
        let mut partial_idx = 0;
        let mut offset = 0;
        let mut new_chunk = true;
        while chunk_idx < chunks_offsets.len() && partial_idx < partial_unmodified.len() {
            // here is '<', not '<=', chunks_offsets[chunk_idx] is the count of chunks[chunk_idx]
            if partial_unmodified[partial_idx].0.1 < chunks_offsets[chunk_idx] {
                if new_chunk {
                    res.push((Vec::new(), chunk_idx as u64));
                    offset = res.len() - 1;
                    new_chunk = false;
                }
                res[offset].0.push(partial_unmodified[partial_idx])
            } else {
                new_chunk = true;
                chunk_idx += 1;
                partial_idx -= 1;
            }
            partial_idx += 1;
        }
        // check
        for chunk in &res {
            assert!(!chunk.0.is_empty());
        }
        res
    }
}

/// we think the build blocks count is about 1024 at most time.
impl Default for MergeIntoBlockInfoIndex {
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
    // let's build [0,10][11,20][21,30],[31,39],and then find [10,37].
    // we should get [10,10],[31,37]
    let intervals: Vec<Interval> = vec![(0, 10), (11, 20), (21, 30), (31, 39)];
    let find_interval: Interval = (10, 37);
    let mut block_info_index = MergeIntoBlockInfoIndex::new_with_capacity(10);
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

    // test `gather_all_partial_block_offsets`
    let mut hits = vec![0; 40];
    // [0,9][28,39]
    for item in hits.iter_mut().take(27 + 1).skip(10) {
        *item = 1;
    }
    let result = block_info_index.gather_all_partial_block_offsets(&hits);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, (0, 9));
    assert_eq!(result[0].1, 0);
    assert_eq!(result[1].0, (28, 30));
    assert_eq!(result[1].1, 2);

    let mut hits = vec![0; 40];
    // [0,9]
    for item in hits.iter_mut().take(30 + 1).skip(10) {
        *item = 1;
    }
    let result = block_info_index.gather_all_partial_block_offsets(&hits);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, (0, 9));
    assert_eq!(result[0].1, 0);

    let mut hits = vec![0; 40];
    // [0,10]
    for item in hits.iter_mut().take(30 + 1).skip(11) {
        *item = 1;
    }
    let result = block_info_index.gather_all_partial_block_offsets(&hits);
    assert_eq!(result.len(), 0);

    // test chunk_offsets
    // blocks: [0,10][11,20][21,30],[31,39]
    // chunks: [0,20],[21,39]
    // chunks_offsets: [21],[40]
    // partial_unmodified: [((8,10),0),((13,16),1),((33,36),3)]
    let partial_unmodified = vec![((8, 10), 0), ((13, 16), 1), ((33, 36), 3)];
    let chunks_offsets = vec![21, 40];
    let res = block_info_index.chunk_offsets(&partial_unmodified, &chunks_offsets);
    assert_eq!(res.len(), 2);

    assert_eq!(res[0].0.len(), 2);
    assert_eq!(res[0].1, 0); // chunk_idx
    assert_eq!(res[0].0[0], ((8, 10), 0));
    assert_eq!(res[0].0[1], ((13, 16), 1));

    assert_eq!(res[1].0.len(), 1);
    assert_eq!(res[1].1, 1); // chunk_idx
    assert_eq!(res[1].0[0], ((33, 36), 3));

    // test only one chunk
    // blocks: [0,10][11,20][21,30],[31,39]
    // chunks: [0,20],[21,39]
    // chunks_offsets: [21],[40]
    // partial_unmodified: [((8,10),0),((13,16),1),((33,36),3)]
    let partial_unmodified = vec![((13, 16), 1)];
    let chunks_offsets = vec![21, 40];
    let res = block_info_index.chunk_offsets(&partial_unmodified, &chunks_offsets);
    assert_eq!(res.len(), 1);

    assert_eq!(res[0].0.len(), 1);
    assert_eq!(res[0].1, 0); // chunk_idx
    assert_eq!(res[0].0[0], ((13, 16), 1));

    // test matched all blocks
    // blocks: [0,10][11,20][21,30],[31,39]

    // 1.empty
    let mut hits = vec![0; 40];
    // set [11,19]
    for item in hits.iter_mut().take(19 + 1).skip(11) {
        *item = 1;
    }
    let res = block_info_index.gather_matched_all_blocks(&hits);
    assert!(res.is_empty());

    let mut hits = vec![0; 40];
    // set [13,28]
    for item in hits.iter_mut().take(28 + 1).skip(13) {
        *item = 1;
    }
    let res = block_info_index.gather_matched_all_blocks(&hits);
    assert!(res.is_empty());

    // 2.one
    let mut hits = vec![0; 40];
    // set [11,20]
    for item in hits.iter_mut().take(20 + 1).skip(11) {
        *item = 1;
    }
    let res = block_info_index.gather_matched_all_blocks(&hits);
    assert!(res.len() == 1 && res[0] == 1);

    let mut hits = vec![0; 40];
    // set [13,33]
    for item in hits.iter_mut().take(33 + 1).skip(13) {
        *item = 1;
    }
    let res = block_info_index.gather_matched_all_blocks(&hits);
    assert!(res.len() == 1 && res[0] == 2);

    // 3.multi blocks
    let mut hits = vec![0; 40];
    // set [11,30]
    for item in hits.iter_mut().take(30 + 1).skip(11) {
        *item = 1;
    }
    let res = block_info_index.gather_matched_all_blocks(&hits);
    assert!(res.len() == 2 && res[0] == 1 && res[1] == 2);

    let mut hits = vec![0; 40];
    // set [10,31]
    for item in hits.iter_mut().take(31 + 1).skip(11) {
        *item = 1;
    }
    let res = block_info_index.gather_matched_all_blocks(&hits);
    assert!(res.len() == 2 && res[0] == 1 && res[1] == 2);
}

#[test]
fn test_chunk_offsets_skip_chunk() {
    // test chunk_offsets
    // blocks: [0,10],[11,20],[21,30],[31,39],[40,50],[51,60]
    // chunks: [0,20],[21,39],[40,60]
    // chunks_offsets: [21],[40],[61]
    // partial_unmodified: [((8,10),0),((40,46),4),((51,55),5)]
    let partial_unmodified = vec![((8, 10), 0), ((40, 46), 4), ((51, 55), 5)];
    let chunks_offsets = vec![21, 40, 61];
    let intervals: Vec<Interval> = vec![(0, 10), (11, 20), (21, 30), (31, 39), (40, 50), (51, 60)];
    let mut block_info_index = MergeIntoBlockInfoIndex::new_with_capacity(10);
    for (idx, interval) in intervals.iter().enumerate() {
        block_info_index.insert_block_offsets(*interval, idx as u64)
    }
    let res = block_info_index.chunk_offsets(&partial_unmodified, &chunks_offsets);
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].0.len(), 1);
    assert_eq!(res[0].0[0].0.0, 8);
    assert_eq!(res[0].0[0].0.1, 10);

    assert_eq!(res[1].0.len(), 2);
    assert_eq!(res[1].0[0].0.0, 40);
    assert_eq!(res[1].0[0].0.1, 46);

    assert_eq!(res[1].0[1].0.0, 51);
    assert_eq!(res[1].0[1].0.1, 55);
}
