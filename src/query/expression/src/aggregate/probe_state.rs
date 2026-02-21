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

use std::ops::Index;
use std::ops::IndexMut;

use super::BATCH_SIZE;
use super::StateAddr;
use super::row_ptr::RowPtr;

pub type SelectVector = [RowID; BATCH_SIZE];

/// ProbeState is the state to probe HT
/// It could be reuse during multiple probe process
#[derive(Debug)]
pub struct ProbeState {
    pub(super) group_hashes: [u64; BATCH_SIZE],
    pub(super) addresses: [RowPtr; BATCH_SIZE],
    pub(super) page_index: [usize; BATCH_SIZE],
    pub(super) state_places: [StateAddr; BATCH_SIZE],

    pub(super) empty_vector: SelectVector,

    pub(super) group_compare_vector: [RowID; BATCH_SIZE],
    pub(super) no_match_vector: [RowID; BATCH_SIZE],
    pub(super) slots: [usize; BATCH_SIZE],
    pub(super) row_count: usize,

    pub partition_entries: Vec<(u16, SelectVector)>,
}

impl Default for ProbeState {
    fn default() -> Self {
        Self {
            group_hashes: [0; BATCH_SIZE],
            addresses: [RowPtr::null(); BATCH_SIZE],
            page_index: [0; BATCH_SIZE],
            state_places: [StateAddr::null(); BATCH_SIZE],
            group_compare_vector: [RowID::default(); BATCH_SIZE],
            no_match_vector: [RowID::default(); BATCH_SIZE],
            empty_vector: [RowID::default(); BATCH_SIZE],
            slots: [0; BATCH_SIZE],

            row_count: 0,
            partition_entries: vec![],
        }
    }
}

unsafe impl Send for ProbeState {}
unsafe impl Sync for ProbeState {}

impl ProbeState {
    pub fn new_boxed() -> Box<ProbeState> {
        let mut state = Box::<ProbeState>::new_zeroed();
        unsafe {
            let uninit = state.assume_init_mut();
            uninit.partition_entries = vec![];

            for ptr in &mut uninit.addresses {
                *ptr = RowPtr::null();
            }
            for addr in &mut uninit.state_places {
                *addr = StateAddr::null()
            }
            for item in &mut uninit.group_compare_vector {
                *item = Default::default()
            }
            for item in &mut uninit.no_match_vector {
                *item = Default::default()
            }
            state.assume_init()
        }
    }

    pub(super) fn reset_partitions(&mut self, partition_count: usize) {
        if partition_count > self.partition_entries.capacity() {
            self.partition_entries
                .reserve(partition_count - self.partition_entries.capacity());
        }
        unsafe {
            self.partition_entries.set_len(partition_count);
        }
        for (count, _) in &mut self.partition_entries {
            *count = 0;
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowID(pub u16);

impl RowID {
    #[inline]
    pub fn to_usize(self) -> usize {
        self.0 as usize
    }
}

impl<T> Index<RowID> for [T] {
    type Output = T;

    fn index(&self, index: RowID) -> &T {
        let i = index.0 as usize;
        debug_assert!(
            i < self.len(),
            "RowID out of bounds: index={} len={}",
            i,
            self.len()
        );
        unsafe { self.get_unchecked(i) }
    }
}

impl<T> IndexMut<RowID> for [T] {
    fn index_mut(&mut self, index: RowID) -> &mut T {
        let i = index.0 as usize;
        debug_assert!(
            i < self.len(),
            "RowID out of bounds: index={} len={}",
            i,
            self.len()
        );
        unsafe { self.get_unchecked_mut(i) }
    }
}

impl<T> Index<RowID> for Vec<T> {
    type Output = T;

    fn index(&self, index: RowID) -> &T {
        let i = index.0 as usize;
        debug_assert!(
            i < self.len(),
            "RowID out of bounds: index={} len={}",
            i,
            self.len()
        );
        unsafe { self.get_unchecked(i) }
    }
}

impl From<usize> for RowID {
    fn from(v: usize) -> Self {
        RowID(v as u16)
    }
}
