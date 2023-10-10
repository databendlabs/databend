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

use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use serde::Deserialize;
use serde::Serialize;

use crate::utils::arrow::buffer_into_mut;

#[derive(Clone, PartialEq)]
pub struct FixedStringColumn {
    data: Buffer<u8>,
    fixed_size: usize,
}

impl FixedStringColumn {
    pub fn new(data: Buffer<u8>, fixed_size: usize) -> Self {
        Self { data, fixed_size }
    }

    pub fn len(&self) -> usize {
        self.data().len() / self.fixed_size
    }

    pub fn fixed_size(&self) -> usize {
        self.fixed_size
    }

    pub fn data(&self) -> &Buffer<u8> {
        &self.data
    }

    pub fn memory_size(&self) -> usize {
        self.data.len()
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(range) }
    }

    /// Slices this [`FixedSizeBinaryArray`].
    /// # Implementation
    /// This operation is `O(1)`.
    /// # Safety
    pub unsafe fn slice_unchecked(&self, range: Range<usize>) -> Self {
        let mut data = self.data.clone();
        data.slice_unchecked(
            range.start * self.fixed_size,
            (range.end - range.start + 1) * self.fixed_size,
        );

        Self {
            data,
            fixed_size: self.fixed_size,
        }
    }

    pub fn index(&self, index: usize) -> Option<&[u8]> {
        if index < self.len() {
            Some(&self.data[(index * self.fixed_size)..(index + 1) * self.fixed_size])
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &[u8] {
        &self.data[(index * self.fixed_size)..(index + 1) * self.fixed_size]
    }
}

impl<'a> FixedStringColumn {
    /// Returns iterator over the values of [`FixedSizeBinaryArray`]
    pub fn iter(&'a self) -> std::slice::ChunksExact<'a, u8> {
        self.data.chunks_exact(self.fixed_size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedStringColumnBuilder {
    // if the StringColumnBuilder is created with `data_capacity`, need_estimated is false
    pub data: Vec<u8>,
    pub fixed_size: usize,
}

impl FixedStringColumnBuilder {
    pub fn with_capacity(len: usize, fixed_size: usize) -> Self {
        FixedStringColumnBuilder {
            data: Vec::with_capacity(len * fixed_size),
            fixed_size,
        }
    }

    pub fn from_column(col: FixedStringColumn) -> Self {
        FixedStringColumnBuilder {
            data: buffer_into_mut(col.data),
            fixed_size: col.fixed_size,
        }
    }

    pub fn put(&mut self, item: &[u8]) {
        self.data.extend_from_slice(item);
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if self.data.len() >= self.fixed_size {
            let val = self.data.split_off(self.data.len() - self.fixed_size);
            Some(val)
        } else {
            None
        }
    }

    pub fn build(self) -> FixedStringColumn {
        debug_assert!(self.data.len() % self.fixed_size == 0);
        let data = self.data.into();
        FixedStringColumn::new(data, self.fixed_size)
    }

    pub fn build_scalar(self) -> Vec<u8> {
        self.data
    }
}
