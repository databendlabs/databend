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

use std::iter::TrustedLen;
use std::marker::PhantomData;

use databend_common_arrow::arrow::buffer::Buffer;

use crate::types::binary::BinaryColumnBuilder;

#[derive(Debug, Clone)]
pub struct BinaryState {
    pub data: Buffer<u8>,
    pub offsets: Buffer<u64>,
}

impl BinaryState {
    pub fn iter(&self) -> BinaryLikeIterator<'_, &[u8]> {
        BinaryLikeIterator {
            data: &self.data,
            offsets: self.offsets.windows(2),
            _t: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn index(&self, index: usize) -> Option<&[u8]> {
        if index + 1 < self.offsets.len() {
            Some(&self.data[(self.offsets[index] as usize)..(self.offsets[index + 1] as usize)])
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, row: usize) -> &[u8] {
        debug_assert!(row + 1 < self.offsets.len());

        let start = *self.offsets.get_unchecked(row) as usize;
        let end = *self.offsets.get_unchecked(row + 1) as usize;
        self.data.get_unchecked(start..end)
    }

    pub fn into_buffer(self) -> (Buffer<u8>, Buffer<u64>) {
        (self.data, self.offsets)
    }
}

pub type BinaryStateBuilder = BinaryColumnBuilder;

pub type BinaryStateIterator<'a> = BinaryLikeIterator<'a, &'a [u8]>;

pub trait BinaryLike<'a> {
    fn from(value: &'a [u8]) -> Self;
}

impl<'a> BinaryLike<'a> for &'a [u8] {
    fn from(value: &'a [u8]) -> Self {
        value
    }
}

pub struct BinaryLikeIterator<'a, T>
where T: BinaryLike<'a>
{
    pub(crate) data: &'a [u8],
    pub(crate) offsets: std::slice::Windows<'a, u64>,
    pub(crate) _t: PhantomData<T>,
}

impl<'a, T: BinaryLike<'a>> Iterator for BinaryLikeIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.offsets
            .next()
            .map(|range| T::from(&self.data[(range[0] as usize)..(range[1] as usize)]))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

unsafe impl<'a, T: BinaryLike<'a>> TrustedLen for BinaryLikeIterator<'a, T> {}
