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

use std::alloc::Allocator;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;

use databend_common_base::mem_allocator::MmapAllocator;

use super::traits::Keyable;
use crate::HashJoinHashtableLike;

// This hashtable is only used for target build merge into (both standalone and distributed mode).
// Advantages:
//      1. Reduces redundant I/O operations, enhancing performance.
//      2. Lowers the maintenance overhead of deduplicating row_id.
//      3. Allows the scheduling of the subsequent mutation pipeline to be entirely allocated to not matched append operations.
// Disadvantages:
//      1. This solution is likely to be a one-time approach (especially if there are not matched insert operations involved),
// potentially leading to the target table being unsuitable for use as a build table in the future.
//      2. Requires a significant amount of memory to be efficient and currently does not support spill operations.
#[allow(unused)]
pub struct HashJoinBlockInfoHashTable<K: Keyable, A: Allocator + Clone = MmapAllocator> {
    pub(crate) pointers: Box<[u64], A>,
    pub(crate) atomic_pointers: *mut AtomicU64,
    pub(crate) hash_shift: usize,
    pub(crate) phantom: PhantomData<K>,
}

impl<K, A> HashJoinHashtableLike for HashJoinBlockInfoHashTable<K, A>
where
    K: Keyable,
    A: Allocator + Clone + 'static,
{
    type Key = K;
    #[allow(unused)]
    fn probe(
        &self,
        hashes: &mut [u64],
        bitmap: Option<databend_common_arrow::arrow::bitmap::Bitmap>,
    ) -> usize {
        todo!()
    }
    #[allow(unused)]
    fn early_filtering_probe(
        &self,
        hashes: &mut [u64],
        bitmap: Option<databend_common_arrow::arrow::bitmap::Bitmap>,
    ) -> usize {
        todo!()
    }

    #[allow(unused)]
    fn early_filtering_probe_with_selection(
        &self,
        hashes: &mut [u64],
        valids: Option<databend_common_arrow::arrow::bitmap::Bitmap>,
        selection: &mut [u32],
    ) -> usize {
        todo!()
    }

    #[allow(unused)]
    fn next_contains(&self, key: &Self::Key, ptr: u64) -> bool {
        todo!()
    }

    #[allow(unused)]
    fn next_probe(
        &self,
        key: &Self::Key,
        ptr: u64,
        vec_ptr: *mut crate::RowPtr,
        occupied: usize,
        capacity: usize,
    ) -> (usize, u64) {
        todo!()
    }
}
