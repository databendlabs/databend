// Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::buffer::Buffer;

use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;

/// # Safety
///
/// Each item in the `indices` consists of an `index` and a `cnt`, the sum
/// of the `cnt` must be equal to the `row_num`, the out-of-bounds `index`
/// for `col` in indices is *[undefined behavior]*.
pub unsafe fn copy_numeric_by_compressd_indices<T>(
    col: &Buffer<T>,
    indices: &[(u32, u32)],
    row_num: usize,
) -> Vec<T> {
    let mut builder: Vec<T> = Vec::with_capacity(row_num);
    let builder_ptr = builder.as_mut_ptr();
    let col_ptr = col.as_ptr();
    let mut offset = 0;
    let mut remain;
    let mut power;
    for (index, cnt) in indices {
        if *cnt == 1 {
            std::ptr::copy_nonoverlapping(col_ptr.add(*index as usize), builder_ptr.add(offset), 1);
            offset += 1;
            continue;
        }
        // Using the doubling method to copy memory.
        let base_offset = offset;
        std::ptr::copy_nonoverlapping(
            col_ptr.add(*index as usize),
            builder_ptr.add(base_offset),
            1,
        );
        remain = *cnt as usize;
        // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
        let max_segment = 1 << (31 - cnt.leading_zeros());
        let mut cur_segment = 1;
        while cur_segment < max_segment {
            std::ptr::copy_nonoverlapping(
                builder_ptr.add(base_offset),
                builder_ptr.add(base_offset + cur_segment),
                cur_segment,
            );
            cur_segment <<= 1;
        }
        remain -= max_segment;
        offset += max_segment;
        power = 0;
        while remain > 0 {
            if remain & 1 == 1 {
                let cur_segment = 1 << power;
                std::ptr::copy_nonoverlapping(
                    builder_ptr.add(base_offset),
                    builder_ptr.add(offset),
                    cur_segment,
                );
                offset += cur_segment;
            }
            power += 1;
            remain >>= 1;
        }
    }
    builder.set_len(offset);
    builder
}

/// # Safety
///
/// Each item in the `indices` consists of an `index` and a `cnt`, the sum
/// of the `cnt` must be equal to the `row_num`, the out-of-bounds `index`
/// for `col` in indices is *[undefined behavior]*.
pub unsafe fn copy_string_by_compressd_indices<'a>(
    col: &'a StringColumn,
    indices: &[(u32, u32)],
    row_num: usize,
) -> StringColumnBuilder {
    let mut data_capacity: u64 = 0;
    let mut offsets: Vec<u64> = Vec::with_capacity(row_num + 1);
    offsets.push(0);
    for (index, cnt) in indices {
        let col = col.index_unchecked(*index as usize);
        for _ in 0..*cnt {
            data_capacity += col.len() as u64;
            offsets.push(data_capacity);
        }
    }
    let mut col_builder =
        StringColumnBuilder::with_capacity_and_offset(row_num, data_capacity as usize, offsets);
    let builder_ptr = col_builder.as_mut_data_ptr();
    let col_ptr = col.as_data_ptr();
    let mut offset = 0;
    for (index, cnt) in indices {
        let len = col.get_len(*index as usize);
        if *cnt == 1 {
            std::ptr::copy_nonoverlapping(
                col_ptr.add(col.get_offset(*index as usize) as usize),
                builder_ptr.add(offset),
                len,
            );
            offset += len;
            continue;
        }
        // Using the doubling method to copy memory.
        let base_offset = offset;
        std::ptr::copy_nonoverlapping(
            col_ptr.add(col.get_offset(*index as usize) as usize),
            builder_ptr.add(base_offset),
            len,
        );
        // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
        let mut remain = *cnt as usize;
        let max_bit_num = 1 << (31 - cnt.leading_zeros());
        let max_segment = max_bit_num * len;
        let mut cur_segment = len;
        while cur_segment < max_segment {
            std::ptr::copy_nonoverlapping(
                builder_ptr.add(base_offset),
                builder_ptr.add(base_offset + cur_segment),
                cur_segment,
            );
            cur_segment <<= 1;
        }
        remain -= max_bit_num;
        offset += max_segment;
        let mut power = 0;
        while remain > 0 {
            if remain & 1 == 1 {
                let cur_segment = (1 << power) * len;
                std::ptr::copy_nonoverlapping(
                    builder_ptr.add(base_offset),
                    builder_ptr.add(offset),
                    cur_segment,
                );
                offset += cur_segment;
            }
            power += 1;
            remain >>= 1;
        }
    }
    col_builder.data.set_len(offset);
    col_builder
}
