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

use std::cell::Cell;

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
/// with one load of the page address. Ids grow with the pages: the largest id a table may give
/// its rows follows from the number of its pages, see `max_row_id`.
#[derive(Debug)]
pub(super) struct RowIds {
    /// The address of each numbered page, then free entries for the pages a probe numbers. The
    /// entries are cells and the table has room for every page a probe may open, see
    /// `reserve_pages`: the probe numbers pages while it decodes ids from the same table.
    pages: Vec<Cell<*const u8>>,
    /// The numbered pages.
    numbered: usize,
    /// The page numbers of the pages of each partition, in their order in the partition.
    partition_pages: Vec<Vec<u32>>,
    layout: IdLayout,
    /// Pages counted on top of the real ones by `max_row_id`, to reach wide ids with small
    /// tables in tests.
    #[cfg(test)]
    extra_pages: usize,
}

impl RowIds {
    pub(super) fn new(
        partition_count: usize,
        row_per_page: usize,
        row_size: usize,
        states: StatesRegion,
    ) -> Self {
        let row_bits = usize::BITS - (row_per_page.max(2) - 1).leading_zeros();
        assert!(row_bits < u32::BITS);
        RowIds {
            pages: vec![],
            numbered: 0,
            partition_pages: vec![vec![]; partition_count],
            layout: IdLayout {
                row_bits,
                row_mask: (1 << row_bits) - 1,
                row_size,
                states_offset: states.offset,
                state_size: states.size,
            },
            #[cfg(test)]
            extra_pages: 0,
        }
    }

    /// The ids of one numbered page at `page` of rows of `row_size` bytes, without states.
    #[cfg(test)]
    pub(in crate::aggregate) fn single_page(
        page: *const u8,
        row_bits: u32,
        row_size: usize,
    ) -> Self {
        let mut ids = RowIds::new(1, 1 << row_bits, row_size, StatesRegion::new(64, 2, 0, 0));
        ids.pages.push(Cell::new(page));
        ids.numbered = 1;
        ids.partition_pages[0].push(0);
        ids
    }

    /// The largest id of a table of `pages` pages.
    pub(super) fn max_row_id(&self, pages: usize) -> u64 {
        #[cfg(test)]
        let pages = pages + self.extra_pages;
        ((pages.max(1) as u64) << self.layout.row_bits) - 1
    }

    #[cfg(test)]
    pub(super) fn set_extra_pages(&mut self, extra_pages: usize) {
        self.extra_pages = extra_pages;
    }

    /// The pages that take the ids past `bits` bits.
    #[cfg(test)]
    pub(super) fn pages_for_id_bits(&self, bits: u32) -> usize {
        1 << (bits - self.layout.row_bits)
    }

    /// Make room for `pages` numbered pages in all.
    pub(super) fn reserve_pages(&mut self, pages: usize) {
        if self.pages.len() < pages {
            let len = pages.max(self.pages.len() * 2);
            self.pages.resize(len, Cell::new(std::ptr::null()));
        }
    }

    /// Number the pages again from 0, partition by partition.
    pub(super) fn renumber<'a>(&mut self, partitions: impl Iterator<Item = &'a [Page]> + Clone) {
        self.reserve_pages(partitions.clone().map(<[Page]>::len).sum());
        self.numbered = 0;
        self.partition_pages.iter_mut().for_each(Vec::clear);
        let mut numbering = self.numbering();
        for (partition, pages) in partitions.enumerate() {
            if let Some(last) = pages.len().checked_sub(1) {
                numbering.number_pages(partition, pages, last);
            }
        }
    }

    /// The id of the first row of page `page` of `partition`, which has a number.
    #[inline]
    pub(super) fn first_row_id(&self, partition: usize, page: usize) -> u64 {
        (self.partition_pages[partition][page] as u64) << self.layout.row_bits
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
            layout: self.layout,
        }
    }

    /// Numbers pages, see `PageNumbering`.
    pub(super) fn numbering(&mut self) -> PageNumbering<'_> {
        self.split().1
    }

    /// Decodes ids and numbers pages at once, for a probe.
    pub(in crate::aggregate) fn split(&mut self) -> (RowDecoder<'_>, PageNumbering<'_>) {
        let numbering = PageNumbering {
            pages: &self.pages,
            numbered: &mut self.numbered,
            partition_pages: &mut self.partition_pages,
            row_bits: self.layout.row_bits,
        };
        let decoder = RowDecoder {
            pages: &self.pages,
            layout: self.layout,
        };
        (decoder, numbering)
    }
}

/// Numbers the pages of `RowIds` in the entries it reserved.
pub(in crate::aggregate) struct PageNumbering<'a> {
    pages: &'a [Cell<*const u8>],
    numbered: &'a mut usize,
    partition_pages: &'a mut [Vec<u32>],
    row_bits: u32,
}

impl PageNumbering<'_> {
    /// The id of the first row of page `page` of `partition`, whose pages are `pages`. Numbers the
    /// pages of the partition up to it that have no number yet.
    #[inline]
    pub(super) fn number_pages(&mut self, partition: usize, pages: &[Page], page: usize) -> u64 {
        let numbers = &mut self.partition_pages[partition];
        while numbers.len() <= page {
            // Out of the reserved entries panics: a probe must not move the table.
            self.pages[*self.numbered].set(pages[numbers.len()].base());
            numbers.push(*self.numbered as u32);
            *self.numbered += 1;
        }
        debug_assert_eq!(self.pages[numbers[page] as usize].get(), pages[page].base());
        (numbers[page] as u64) << self.row_bits
    }
}

/// How an id names its row and states in its page.
#[derive(Clone, Copy, Debug)]
struct IdLayout {
    row_bits: u32,
    row_mask: u64,
    row_size: usize,
    states_offset: usize,
    state_size: usize,
}

/// Decodes row ids into the addresses of their rows and states. It is a copy of what it needs
/// out of `RowIds`, so that a probe loop keeps it in registers across its stores.
#[derive(Clone, Copy)]
pub(in crate::aggregate) struct RowDecoder<'a> {
    pages: &'a [Cell<*const u8>],
    layout: IdLayout,
}

impl RowDecoder<'_> {
    /// The row with id `id`.
    #[inline]
    pub(in crate::aggregate) fn row(self, id: u64) -> RowPtr {
        let IdLayout {
            row_bits,
            row_mask,
            row_size,
            ..
        } = self.layout;
        let page = self.pages[(id >> row_bits) as usize].get();
        RowPtr::new(page.wrapping_add((id & row_mask) as usize * row_size))
    }

    /// The row with id `id` and its states, found from the page and the row in it: the states of
    /// the rows of a page are in the same order as the rows.
    #[inline]
    pub(in crate::aggregate) fn row_and_states(self, id: u64) -> (RowPtr, StateAddr) {
        let IdLayout {
            row_bits,
            row_mask,
            row_size,
            states_offset,
            state_size,
            ..
        } = self.layout;
        let page = self.pages[(id >> row_bits) as usize].get();
        let row = (id & row_mask) as usize;
        let states = page.wrapping_add(states_offset + row * state_size);
        (
            RowPtr::new(page.wrapping_add(row * row_size)),
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
