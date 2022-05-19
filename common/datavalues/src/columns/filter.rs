// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::utils::BitChunkIterExact;
use common_arrow::arrow::bitmap::utils::BitChunksExact;

use crate::prelude::*;

// Not covered in Array/Struct Columns
impl Series {
    pub(crate) fn filter_column<C: ScalarColumn>(c: &C, filter: &BooleanColumn) -> ColumnRef {
        let data_type = c.data_type();
        if data_type.data_type_id() == TypeID::Array {
            let col = c.as_any().downcast_ref::<ArrayColumn>().unwrap();
            return col.filter(filter);
        } else if data_type.data_type_id() == TypeID::Struct {
            let col = c.as_any().downcast_ref::<StructColumn>().unwrap();
            return col.filter(filter);
        } else {
            let length = filter.values().len() - filter.values().null_count();
            if length == c.len() {
                return c.clone_column();
            }
            const CHUNK_SIZE: usize = 64;
            let mut builder = <<C as ScalarColumn>::Builder>::with_capacity(c.len());

            let (mut slice, offset, mut length) = filter.values().as_slice();
            let mut start_index: usize = 0;

            if offset > 0 {
                let n = 8 - offset;
                start_index += n;

                filter
                    .values()
                    .iter()
                    .enumerate()
                    .take(n)
                    .for_each(|(idx, is_selected)| {
                        if is_selected {
                            builder.push(c.get_data(idx));
                        }
                    });
                slice = &slice[1..];
                length -= n;
            }

            let mut mask_chunks = BitChunksExact::<u64>::new(slice, length);

            mask_chunks
                .by_ref()
                .enumerate()
                .for_each(|(mask_index, mut mask)| {
                    while mask != 0 {
                        let n = mask.trailing_zeros() as usize;
                        let i = mask_index * CHUNK_SIZE + n + start_index;
                        builder.push(c.get_data(i));
                        mask = mask & (mask - 1);
                    }
                });

            let remainder_start = length - length % CHUNK_SIZE;
            mask_chunks
                .remainder_iter()
                .enumerate()
                .for_each(|(mask_index, is_selected)| {
                    if is_selected {
                        let i = mask_index + remainder_start + start_index;
                        builder.push(c.get_data(i));
                    }
                });
            builder.to_column()
        }
    }
}
