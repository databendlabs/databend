// Copyright (c) 2020 Ritchie Vink
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

use std::fmt::Debug;
use std::iter::once;

use serde::Deserialize;
use serde::Serialize;

use super::BinaryColumn;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryColumnBuilder {
    // if the BinaryColumnBuilder is created with `data_capacity`, need_estimated is false
    pub need_estimated: bool,
    pub data: Vec<u8>,
    pub offsets: Vec<u64>,
}

impl BinaryColumnBuilder {
    pub fn with_capacity(len: usize, data_capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(0);
        BinaryColumnBuilder {
            need_estimated: data_capacity == 0 && len > 0,
            data: Vec::with_capacity(data_capacity),
            offsets,
        }
    }

    pub fn from_column(col: BinaryColumn) -> Self {
        BinaryColumnBuilder {
            need_estimated: col.data.is_empty(),
            data: col.data.make_mut(),
            offsets: col.offsets.to_vec(),
        }
    }

    pub fn from_data(data: Vec<u8>, offsets: Vec<u64>) -> Self {
        debug_assert!({ offsets.windows(2).all(|w| w[0] <= w[1]) });

        BinaryColumnBuilder {
            need_estimated: false,
            data,
            offsets,
        }
    }

    pub fn repeat(scalar: &[u8], n: usize) -> Self {
        let len = scalar.len();
        let data = scalar.repeat(n);
        let offsets = once(0)
            .chain((0..n).map(|i| (len * (i + 1)) as u64))
            .collect();
        BinaryColumnBuilder {
            data,
            offsets,
            need_estimated: false,
        }
    }

    pub fn repeat_default(n: usize) -> Self {
        BinaryColumnBuilder {
            data: vec![],
            offsets: vec![0; n + 1],
            need_estimated: false,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
    }

    pub fn memory_size(&self) -> usize {
        self.offsets.len() * 8 + self.data.len()
    }

    pub fn put_u8(&mut self, item: u8) {
        self.data.push(item);
    }

    pub fn put_char(&mut self, item: char) {
        self.data
            .extend_from_slice(item.encode_utf8(&mut [0; 4]).as_bytes());
    }

    #[inline]
    pub fn put_str(&mut self, item: &str) {
        self.data.extend_from_slice(item.as_bytes());
    }

    #[inline]
    pub fn put_slice(&mut self, item: &[u8]) {
        self.data.extend_from_slice(item);
    }

    pub fn put_char_iter(&mut self, iter: impl Iterator<Item = char>) {
        for c in iter {
            let mut buf = [0; 4];
            let result = c.encode_utf8(&mut buf);
            self.data.extend_from_slice(result.as_bytes());
        }
    }

    pub fn put(&mut self, item: &[u8]) {
        self.data.extend_from_slice(item);
    }

    #[inline]
    pub fn commit_row(&mut self) {
        self.offsets.push(self.data.len() as u64);

        if self.need_estimated
            && self.offsets.len() - 1 == 64
            && self.offsets.len() < self.offsets.capacity()
        {
            let bytes_per_row = self.data.len() / 64 + 1;
            let bytes_estimate = bytes_per_row * self.offsets.capacity();

            const MAX_HINT_SIZE: usize = 1_000_000_000;
            // if we are more than 10% over the capacity, we reserve more
            if bytes_estimate < MAX_HINT_SIZE
                && bytes_estimate as f64 > self.data.capacity() as f64 * 1.10f64
            {
                self.data.reserve(bytes_estimate - self.data.capacity());
            }
        }
    }

    pub fn append_column(&mut self, other: &BinaryColumn) {
        // the first offset of other column may not be zero
        let other_start = *other.offsets.first().unwrap();
        let other_last = *other.offsets.last().unwrap();
        let start = self.offsets.last().cloned().unwrap();
        self.data
            .extend_from_slice(&other.data[(other_start as usize)..(other_last as usize)]);
        self.offsets.extend(
            other
                .offsets
                .iter()
                .skip(1)
                .map(|offset| start + offset - other_start),
        );
    }

    pub fn build(self) -> BinaryColumn {
        BinaryColumn::new(self.data.into(), self.offsets.into())
    }

    pub fn build_scalar(self) -> Vec<u8> {
        assert_eq!(self.offsets.len(), 2);

        self.data[(self.offsets[0] as usize)..(self.offsets[1] as usize)].to_vec()
    }

    #[inline]
    pub fn may_resize(&self, add_size: usize) -> bool {
        self.data.len() + add_size > self.data.capacity()
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, row: usize) -> &[u8] { unsafe {
        debug_assert!(row + 1 < self.offsets.len());

        let start = *self.offsets.get_unchecked(row) as usize;
        let end = *self.offsets.get_unchecked(row + 1) as usize;
        self.data.get_unchecked(start..end)
    }}

    pub fn push_repeat(&mut self, item: &[u8], n: usize) {
        self.data.reserve(item.len() * n);
        if self.need_estimated && self.offsets.len() - 1 < 64 {
            for _ in 0..n {
                self.data.extend_from_slice(item);
                self.commit_row();
            }
        } else {
            let start = self.data.len();
            let len = item.len();
            for _ in 0..n {
                self.data.extend_from_slice(item)
            }
            self.offsets
                .extend((1..=n).map(|i| (start + len * i) as u64));
        }
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if !self.is_empty() {
            let index = self.len() - 1;
            let start = unsafe { *self.offsets.get_unchecked(index) as usize };
            self.offsets.pop();
            let val = self.data.split_off(start);
            Some(val)
        } else {
            None
        }
    }

    /// Extends the [`MutableBinaryArray`] from an iterator of values.
    /// This differs from `extended_trusted_len` which accepts iterator of optional values.
    #[inline]
    pub fn extend_values<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = P>,
    {
        for item in iterator {
            self.put_slice(item.as_ref());
            self.commit_row();
        }
    }

    pub fn reserve(&mut self, offset_additional: usize, data_additional: usize) {
        self.offsets.reserve(offset_additional);
        self.data.reserve(data_additional)
    }
}

impl<P: AsRef<[u8]>> FromIterator<P> for BinaryColumnBuilder {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut builder = BinaryColumnBuilder::with_capacity(lower, 0);
        builder.extend_values(iter);
        builder
    }
}

impl From<BinaryColumnBuilder> for BinaryColumn {
    fn from(value: BinaryColumnBuilder) -> Self {
        value.build()
    }
}
