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

/// The id of a row of a partitioned payload, kept by the hash index in place of a row address:
/// from the low bits up, the partition, the row in the page, and the page. Ids grow with the page,
/// so a table whose pages stay within `max_compact_page` has ids that fit 4 bytes.
#[derive(Clone, Copy, Debug)]
pub(super) struct RowIdCodec {
    partition_bits: u32,
    partition_mask: u64,
    row_bits: u32,
    row_mask: u64,
    max_compact_page: usize,
}

impl RowIdCodec {
    pub(super) fn new(partition_count: usize, row_per_page: usize) -> Self {
        debug_assert!(partition_count.is_power_of_two());
        let partition_bits = partition_count.trailing_zeros();
        let row_bits = usize::BITS - (row_per_page.max(2) - 1).leading_zeros();
        assert!(partition_bits + row_bits < u32::BITS);
        RowIdCodec {
            partition_bits,
            partition_mask: (1 << partition_bits) - 1,
            row_bits,
            row_mask: (1 << row_bits) - 1,
            max_compact_page: (1 << (u32::BITS - partition_bits - row_bits)) - 1,
        }
    }

    /// The largest page index whose ids fit 4 bytes.
    pub(super) fn max_compact_page(&self) -> usize {
        self.max_compact_page
    }

    /// Lower the pages whose ids count as compact, to reach wide ids with small tables in tests.
    #[cfg(test)]
    pub(super) fn with_max_compact_page(mut self, max_compact_page: usize) -> Self {
        self.max_compact_page = max_compact_page;
        self
    }

    #[inline]
    pub(super) fn encode(&self, partition: usize, page: usize, row: usize) -> u64 {
        debug_assert!((page as u64) < 1 << (u64::BITS - self.partition_bits - self.row_bits));
        ((((page as u64) << self.row_bits) | row as u64) << self.partition_bits) | partition as u64
    }

    /// (partition, page, row) of `id`.
    #[inline]
    pub(super) fn decode(&self, id: u64) -> (usize, usize, usize) {
        let partition = (id & self.partition_mask) as usize;
        let id = id >> self.partition_bits;
        (
            partition,
            (id >> self.row_bits) as usize,
            (id & self.row_mask) as usize,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_id_codec_round_trip() {
        for (partition_count, row_per_page) in [(1, 1), (4, 8192), (4, 10922), (128, 65535)] {
            let codec = RowIdCodec::new(partition_count, row_per_page);
            let max_compact_page = codec.max_compact_page();
            for partition in [0, partition_count - 1] {
                for page in [0, 1, max_compact_page, max_compact_page + 1, 1 << 40] {
                    for row in [0, row_per_page - 1] {
                        let id = codec.encode(partition, page, row);
                        assert_eq!(codec.decode(id), (partition, page, row));
                        // Ids fit 4 bytes exactly while the page stays compact.
                        assert_eq!(id <= u32::MAX as u64, page <= max_compact_page);
                    }
                }
            }
        }
    }
}
