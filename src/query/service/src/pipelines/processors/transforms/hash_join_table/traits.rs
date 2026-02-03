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
use databend_common_hashtable::RowPtr;

pub trait HashJoinHashtableLike {
    type Key: ?Sized;

    // Probe hash table, use `hashes` to probe hash table and convert it in-place to pointers for memory reuse.
    fn probe(&self, hashes: &mut [u64], bitmap: Option<Bitmap>) -> usize;

    // Perform early filtering probe, store matched indexes in `matched_selection` and store unmatched indexes
    // in `unmatched_selection`, return the number of matched and unmatched indexes.
    fn early_filtering_probe(
        &self,
        hashes: &mut [u64],
        valids: Option<Bitmap>,
        matched_selection: &mut Vec<u32>,
        unmatched_selection: &mut Vec<u32>,
    ) -> (usize, usize);

    // Perform early filtering probe and store matched indexes in `selection`, return the number of matched indexes.
    fn early_filtering_matched_probe(
        &self,
        hashes: &mut [u64],
        valids: Option<Bitmap>,
        selection: &mut Vec<u32>,
    ) -> usize;

    // we use `next_contains` to see whether we can find a matched row in the link.
    // the ptr is the link header.
    fn next_contains(&self, key: &Self::Key, ptr: u64) -> bool;

    /// 1. `key` is the serialize probe key from one row
    /// 2. `ptr` pointers to the *RawEntry for of the bucket correlated to key.So before this method,
    ///    we will do a round probe firstly. If the ptr is zero, it means there is no correlated bucket
    ///    for key
    /// 3. `vec_ptr` is RowPtr Array, we use this one to record the matched row in chunks
    /// 4. `occupied` is the length for vec_ptr
    /// 5. `capacity` is the capacity of vec_ptr
    /// 6. return matched rows count and next ptr which need to test in the future.
    ///    if the capacity is enough, the next ptr is zero, otherwise next ptr is valid.
    ///
    /// # Safety
    /// Caller must ensure vec_ptr points to a valid RowPtr array with at least `capacity` elements.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn next_probe(
        &self,
        key: &Self::Key,
        ptr: u64,
        vec_ptr: *mut RowPtr,
        occupied: usize,
        capacity: usize,
    ) -> (usize, u64);

    // Find the next matched ptr.
    fn next_matched_ptr(&self, key: &Self::Key, ptr: u64) -> u64;
}
