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

use super::RowID;
use super::StateAddr;
use super::StatesLayout;
use super::payload::Page;
use crate::types::StringColumn;

/// A read-only row pointer into aggregate payload storage.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RowPtr(*const u8);

impl RowPtr {
    pub(super) fn new(ptr: *const u8) -> Self {
        Self(ptr)
    }

    pub(super) fn null() -> Self {
        Self(std::ptr::null())
    }

    pub(super) fn as_ptr(&self) -> *const u8 {
        self.0
    }

    pub(super) unsafe fn read<T>(&self, offset: usize) -> T {
        unsafe { core::ptr::read_unaligned(self.0.add(offset).cast::<T>()) }
    }

    pub(super) unsafe fn read_bytes(&self, offset: usize) -> &[u8] {
        unsafe {
            let len = self.read::<u32>(offset) as usize;
            let data_ptr = self.read::<u64>(offset + 4) as *const u8;
            std::slice::from_raw_parts(data_ptr, len)
        }
    }

    pub(super) unsafe fn is_bytes_eq(&self, offset: usize, other: &[u8]) -> bool {
        unsafe {
            let scalar = self.read_bytes(offset);
            scalar.len() == other.len() && databend_common_hashtable::fast_memcmp(scalar, other)
        }
    }

    pub(super) unsafe fn eq_string_view(
        &self,
        offset: usize,
        str_view: &StringColumn,
        row: RowID,
    ) -> bool {
        unsafe {
            let row = row.to_usize();
            let v = str_view.views().get_unchecked(row);
            let len = self.read::<u32>(offset);
            if v.length != len {
                return false;
            }
            let scalar = {
                let data_ptr = self.read::<u64>(offset + size_of::<u32>()) as *const u8;
                std::slice::from_raw_parts(data_ptr, len as _)
            };
            let other = v.get_slice_unchecked(str_view.data_buffers());
            databend_common_hashtable::fast_memcmp(scalar, other)
        }
    }

    pub(super) unsafe fn read_bool(&self, offset: usize) -> bool {
        unsafe { self.read::<u8>(offset) != 0 }
    }

    pub(super) fn hash(&self, layout: &RowLayout) -> u64 {
        unsafe { self.read::<u64>(layout.hash_offset) }
    }

    #[inline]
    pub(super) fn state_addr(&self, layout: &RowLayout) -> StateAddr {
        StateAddr::from(layout.states.ptr(self.0))
    }
}

/// A writable row pointer used while constructing aggregate payload rows.
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq)]
pub struct RowMut(*mut u8);

const _: () = {
    assert!(core::mem::size_of::<RowPtr>() == core::mem::size_of::<RowMut>());
    assert!(core::mem::align_of::<RowPtr>() == core::mem::align_of::<RowMut>());
};

impl RowMut {
    pub(super) fn new(ptr: *mut u8) -> Self {
        Self(ptr)
    }

    pub(super) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0
    }

    pub(super) fn dangling() -> Self {
        Self(std::ptr::NonNull::<u8>::dangling().as_ptr())
    }

    pub(super) fn into_ref(self) -> RowPtr {
        RowPtr::new(self.0.cast_const())
    }

    pub(super) unsafe fn write<T: Copy>(&mut self, offset: usize, value: &T) {
        unsafe {
            core::ptr::copy_nonoverlapping(
                value as *const T as *const u8,
                self.0.add(offset),
                size_of::<T>(),
            );
        }
    }

    pub(super) unsafe fn write_bytes(&mut self, offset: usize, value: &[u8]) {
        unsafe {
            self.write(offset, &(value.len() as u32));
            self.write(offset + 4, &(value.as_ptr() as u64));
        }
    }

    pub(super) unsafe fn write_u8(&mut self, offset: usize, value: u8) {
        unsafe {
            self.write::<u8>(offset, &value);
        }
    }

    pub(super) fn set_hash(&mut self, layout: &RowLayout, value: u64) {
        unsafe {
            self.write(layout.hash_offset, &value);
        }
    }
}

/// Where the aggregate states of the rows of a page are. A page is aligned to `page_align`, which
/// is at least its size: it holds its rows from the start, and the states of its rows, in the
/// same order, in a region from `offset`, so that the group keys stay dense.
#[derive(Clone, Copy, Debug)]
pub(super) struct StatesRegion {
    page_align: usize,
    row_size: usize,
    /// `row_size` as a multiplier that divides an offset in the page by it.
    row_size_reciprocal: u64,
    pub(super) offset: usize,
    pub(super) size: usize,
}

impl StatesRegion {
    pub(super) fn new(page_align: usize, row_size: usize, offset: usize, size: usize) -> Self {
        debug_assert!(page_align.is_power_of_two());
        assert!(row_size > 1 && page_align <= u32::MAX as usize);
        StatesRegion {
            page_align,
            row_size,
            row_size_reciprocal: u64::MAX / row_size as u64 + 1,
            offset,
            size,
        }
    }

    pub(super) fn page_align(&self) -> usize {
        self.page_align
    }

    /// The states of the row at `row`.
    #[inline]
    pub(super) fn ptr(&self, row: *const u8) -> *mut u8 {
        let addr = row as usize;
        let page = addr & !(self.page_align - 1);
        // Exact division of a 32-bit offset by a 32-bit row size (Lemire et al., "Faster
        // Remainder by Direct Computation"), without a division instruction.
        let offset = (addr - page) as u64;
        let index = ((offset as u128 * self.row_size_reciprocal as u128) >> 64) as usize;
        debug_assert_eq!(index, offset as usize / self.row_size);
        (page + self.offset + index * self.size) as *mut u8
    }
}

/// The ids of the rows of a partitioned payload, kept by the hash index in place of row
/// addresses: from the low bits up, the row in its page and the page. The pages of all the
/// partitions are numbered in one table in the order they are first met, so that an id decodes
/// with one load of the page address. Ids grow with the pages, so a table of at most
/// `max_compact_pages` pages has ids that fit 4 bytes.
#[derive(Debug)]
pub(super) struct RowIds {
    /// The address of each numbered page.
    pages: Vec<*const u8>,
    /// The page numbers of the pages of each partition, in their order in the partition.
    partition_pages: Vec<Vec<u32>>,
    row_size: usize,
    row_bits: u32,
    row_mask: u64,
    max_compact_pages: usize,
    /// Where the states of the rows of a page are, see `StatesRegion`.
    states: StatesRegion,
    /// Whether the rows have aggregate states.
    has_states: bool,
}

impl RowIds {
    pub(super) fn new(
        partition_count: usize,
        row_per_page: usize,
        row_size: usize,
        states: StatesRegion,
        has_states: bool,
    ) -> Self {
        let row_bits = usize::BITS - (row_per_page.max(2) - 1).leading_zeros();
        assert!(row_bits < u32::BITS);
        RowIds {
            pages: vec![],
            partition_pages: vec![vec![]; partition_count],
            row_size,
            row_bits,
            row_mask: (1 << row_bits) - 1,
            max_compact_pages: 1 << (u32::BITS - row_bits),
            states,
            has_states,
        }
    }

    /// The most pages whose ids fit 4 bytes.
    pub(super) fn max_compact_pages(&self) -> usize {
        self.max_compact_pages
    }

    /// Lower the pages whose ids count as compact, to reach wide ids with small tables in tests.
    #[cfg(test)]
    pub(super) fn set_max_compact_pages(&mut self, max_compact_pages: usize) {
        self.max_compact_pages = max_compact_pages;
    }

    /// Number the pages again from 0, partition by partition.
    pub(super) fn renumber<'a>(&mut self, partitions: impl Iterator<Item = &'a [Page]>) {
        self.pages.clear();
        self.partition_pages.iter_mut().for_each(Vec::clear);
        for (partition, pages) in partitions.enumerate() {
            if let Some(last) = pages.len().checked_sub(1) {
                self.number_pages(partition, pages, last);
            }
        }
    }

    /// The id of the first row of page `page` of `partition`, whose pages are `pages`. Numbers the
    /// pages of the partition up to it that have no number yet.
    #[inline]
    pub(super) fn number_pages(&mut self, partition: usize, pages: &[Page], page: usize) -> u64 {
        let numbers = &mut self.partition_pages[partition];
        while numbers.len() <= page {
            numbers.push(self.pages.len() as u32);
            self.pages.push(pages[numbers.len() - 1].base());
        }
        debug_assert_eq!(self.pages[numbers[page] as usize], pages[page].base());
        self.first_row_id(partition, page)
    }

    /// The id of the first row of page `page` of `partition`, which has a number.
    #[inline]
    pub(super) fn first_row_id(&self, partition: usize, page: usize) -> u64 {
        (self.partition_pages[partition][page] as u64) << self.row_bits
    }

    /// The row with id `id`.
    #[inline]
    pub(super) fn row_ptr(&self, id: u64) -> RowPtr {
        self.decoder().row(id)
    }

    /// Decodes the ids, see `RowDecoder`.
    #[inline]
    pub(super) fn decoder(&self) -> RowDecoder<'_> {
        RowDecoder {
            pages: &self.pages,
            row_bits: self.row_bits,
            row_mask: self.row_mask,
            row_size: self.row_size,
            states_offset: self.states.offset,
            state_size: self.states.size,
            has_states: self.has_states,
        }
    }
}

/// Decodes row ids into the addresses of their rows and states. It copies what it needs out of
/// `RowIds`, so that a probe loop keeps it in registers across its stores: the compiler cannot
/// tell that those stores leave the table alone.
#[derive(Clone, Copy)]
pub(in crate::aggregate) struct RowDecoder<'a> {
    pages: &'a [*const u8],
    row_bits: u32,
    row_mask: u64,
    row_size: usize,
    states_offset: usize,
    state_size: usize,
    has_states: bool,
}

impl<'a> RowDecoder<'a> {
    /// A decoder of ids `(page << row_bits) | row`, for rows of `row_size` bytes without states.
    #[cfg(test)]
    pub(in crate::aggregate) fn new(
        pages: &'a [*const u8],
        row_bits: u32,
        row_size: usize,
    ) -> Self {
        RowDecoder {
            pages,
            row_bits,
            row_mask: (1 << row_bits) - 1,
            row_size,
            states_offset: 0,
            state_size: 0,
            has_states: false,
        }
    }

    /// Whether the rows have aggregate states.
    #[inline]
    pub(in crate::aggregate) fn has_states(self) -> bool {
        self.has_states
    }

    /// The row with id `id`.
    #[inline]
    pub(in crate::aggregate) fn row(self, id: u64) -> RowPtr {
        let page = self.pages[(id >> self.row_bits) as usize];
        RowPtr::new(page.wrapping_add((id & self.row_mask) as usize * self.row_size))
    }

    /// The row with id `id` and its states, found from the page and the row in it: the states of
    /// the rows of a page are in the same order as the rows.
    #[inline]
    pub(in crate::aggregate) fn row_and_states(self, id: u64) -> (RowPtr, StateAddr) {
        let page = self.pages[(id >> self.row_bits) as usize];
        let row = (id & self.row_mask) as usize;
        let states = page.wrapping_add(self.states_offset + row * self.state_size);
        (
            RowPtr::new(page.wrapping_add(row * self.row_size)),
            StateAddr::from(states.cast_mut()),
        )
    }
}

#[derive(Clone, Debug)]
pub struct RowLayout {
    pub(super) hash_offset: usize,
    pub(super) states: StatesRegion,
    pub(super) validity_offsets: Vec<usize>,
    pub(super) group_offsets: Vec<usize>,
    pub(super) group_sizes: Vec<usize>,
    pub(super) states_layout: Option<StatesLayout>,
}
