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

use serde::Deserialize;
use serde::Serialize;

use super::FixedSizeBinaryColumn;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedSizeBinaryColumnBuilder {
    pub data: Vec<u8>,
    pub value_length: usize,
}

impl FixedSizeBinaryColumnBuilder {
    pub fn with_capacity(data_capacity: usize, value_length: usize) -> Self {
        FixedSizeBinaryColumnBuilder {
            data: Vec::with_capacity(data_capacity),
            value_length,
        }
    }

    pub fn from_column(col: FixedSizeBinaryColumn) -> Self {
        FixedSizeBinaryColumnBuilder {
            data: col.data.make_mut(),
            value_length: col.value_length,
        }
    }

    pub fn from_data(data: Vec<u8>) -> Self {
        let value_length = data.len();
        FixedSizeBinaryColumnBuilder { data, value_length }
    }

    pub fn repeat(scalar: &[u8], n: usize) -> Self {
        let len = scalar.len();
        let data = scalar.repeat(n);
        FixedSizeBinaryColumnBuilder {
            data,
            value_length: len,
        }
    }

    pub fn repeat_default(n: usize, value_length: usize) -> Self {
        FixedSizeBinaryColumnBuilder {
            data: vec![0; n * value_length],
            value_length,
        }
    }

    pub fn len(&self) -> usize {
        if self.data.is_empty() {
            0
        } else {
            self.data.len() / self.value_length
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() <= 1
    }

    pub fn memory_size(&self) -> usize {
        self.data.len()
    }

    pub fn put_u8(&mut self, item: u8) {
        self.data.push(item);
    }

    pub fn push_default(&mut self) {
        self.data.extend_from_slice(&vec![0; self.value_length]);
    }

    pub fn put_char(&mut self, item: char) {
        self.data
            .extend_from_slice(item.encode_utf8(&mut [0; 4]).as_bytes());
    }

    #[inline]
    pub fn put_str(&mut self, item: &str) {
        debug_assert!(self.value_length == item.as_bytes().len());
        self.data.extend_from_slice(item.as_bytes());
    }

    #[inline]
    pub fn put_slice(&mut self, item: &[u8]) {
        debug_assert!(self.value_length == item.len());
        self.data.extend_from_slice(item);
    }

    #[inline]
    pub fn commit_row(&mut self) {
        self.data.reserve(self.data.capacity());
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

    pub fn append_column(&mut self, other: &FixedSizeBinaryColumn) {
        debug_assert!(other.value_length == self.value_length);
        self.data.extend_from_slice(&other.data);
    }

    pub fn build(self) -> FixedSizeBinaryColumn {
        FixedSizeBinaryColumn::new(self.data.into(), self.value_length)
    }

    pub fn build_scalar(self) -> Vec<u8> {
        if self.data.is_empty() {
            vec![]
        } else {
            self.data[0..self.value_length].to_vec()
        }
    }

    #[inline]
    pub fn may_resize(&self, add_size: usize) -> bool {
        self.data.len() + add_size > self.data.capacity()
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, row: usize) -> &[u8] {
        debug_assert!((row + 1) * self.value_length < self.data.len());

        self.data
            .get_unchecked(row * self.value_length..(row + 1) * self.value_length)
    }

    pub fn push_repeat(&mut self, item: &[u8], n: usize) {
        debug_assert!(item.len() / n == self.data.len());
        self.data.reserve(item.len() * n);
        for _ in 0..n {
            self.data.extend_from_slice(item)
        }
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if !self.is_empty() {
            let val = self.data.split_off(self.len() - 1);
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
}

impl<P: AsRef<[u8]>> FromIterator<P> for FixedSizeBinaryColumnBuilder {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut builder = FixedSizeBinaryColumnBuilder::with_capacity(lower, 0);
        builder.extend_values(iter);
        builder
    }
}

impl From<FixedSizeBinaryColumnBuilder> for FixedSizeBinaryColumn {
    fn from(value: FixedSizeBinaryColumnBuilder) -> Self {
        value.build()
    }
}
