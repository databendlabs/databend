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

use common_exception::Result;

use crate::prelude::*;

impl StringColumn {
    // A helper function that transform StringColumn into StringColumn
    // This is useful because we can write directly into Values buffer without extra copy if we want
    pub fn try_transform<F>(
        from: &StringColumn,
        estimate_bytes: usize,
        mut f: F,
    ) -> Result<StringColumn>
    where
        F: FnMut(&[u8], &mut [u8]) -> Result<usize>,
    {
        let mut values: Vec<u8> = Vec::with_capacity(estimate_bytes);
        let mut offsets: Vec<i64> = Vec::with_capacity(from.len() + 1);
        offsets.push(0);

        let mut offset: usize = 0;
        unsafe {
            for x in from.iter() {
                let bytes = std::slice::from_raw_parts_mut(
                    values.as_mut_ptr().add(offset),
                    values.capacity() - offset,
                );

                match f(x, bytes) {
                    Ok(l) => {
                        offset += l;
                        offsets.push(offset as i64);
                    }

                    Err(e) => return Err(e),
                }
            }
            values.set_len(offset);
            values.shrink_to_fit();

            Ok(StringColumn::from_data_unchecked(
                offsets.into(),
                values.into(),
            ))
        }
    }
}
