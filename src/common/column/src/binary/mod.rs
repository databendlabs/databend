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

mod builder;
pub(crate) mod fmt;
mod iterator;

use std::ops::Range;

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;
pub use builder::BinaryColumnBuilder;
pub use iterator::BinaryColumnBuilderIter;
pub use iterator::BinaryColumnIter;

use crate::bitmap::Bitmap;
use crate::bitmap::utils::BitmapIter;
use crate::bitmap::utils::ZipValidity;
use crate::buffer::Buffer;
use crate::error::Error;
use crate::error::Result;

#[derive(Clone, PartialEq)]
pub struct BinaryColumn {
    pub(crate) data: Buffer<u8>,
    pub(crate) offsets: Buffer<u64>,
}

impl BinaryColumn {
    pub fn new(data: Buffer<u8>, offsets: Buffer<u64>) -> Self {
        debug_assert!({ offsets.windows(2).all(|w| w[0] <= w[1]) });

        BinaryColumn { data, offsets }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() <= 1
    }

    pub fn total_bytes_len(&self) -> usize {
        (*self.offsets().last().unwrap() - *self.offsets().first().unwrap()) as _
    }

    pub fn data(&self) -> &Buffer<u8> {
        &self.data
    }

    pub fn offsets(&self) -> &Buffer<u64> {
        &self.offsets
    }

    pub fn memory_size(&self) -> usize {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        len * 8 + (offsets[len - 1] - offsets[0]) as usize
    }

    pub fn index(&self, index: usize) -> Option<&[u8]> {
        if index + 1 < self.offsets.len() {
            Some(self.value(index))
        } else {
            None
        }
    }

    pub fn value(&self, index: usize) -> &[u8] {
        &self.data[(self.offsets[index] as usize)..(self.offsets[index + 1] as usize)]
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &[u8] {
        unsafe {
            let start = *self.offsets.get_unchecked(index) as usize;
            let end = *self.offsets.get_unchecked(index + 1) as usize;
            // here we use checked slice to avoid UB
            // Less  regressed perfs:
            // bench_kernels/binary_sum_len_unchecked/20
            //                     time:   [45.234 µs 45.278 µs 45.312 µs]
            //                     change: [+1.4430% +1.5796% +1.7344%] (p = 0.00 < 0.05)
            //                     Performance has regressed.
            &self.data[start..end]
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let offsets = self
            .offsets
            .clone()
            .sliced(range.start, range.end - range.start + 1);
        BinaryColumn {
            data: self.data.clone(),
            offsets,
        }
    }

    pub fn iter(&self) -> BinaryColumnIter<'_> {
        BinaryColumnIter::new(self)
    }

    pub fn option_iter<'a>(
        &'a self,
        validity: Option<&'a Bitmap>,
    ) -> ZipValidity<&'a [u8], BinaryColumnIter<'a>, BitmapIter<'a>> {
        let bitmap_iter = validity.as_ref().map(|v| v.iter());
        ZipValidity::new(self.iter(), bitmap_iter)
    }

    pub fn into_buffer(self) -> (Buffer<u8>, Buffer<u64>) {
        (self.data, self.offsets)
    }

    pub fn check_valid(&self) -> Result<()> {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        if len < 1 {
            return Err(Error::OutOfSpec(format!(
                "BinaryColumn offsets length must be equal or greater than 1, but got {}",
                len
            )));
        }

        for i in 1..len {
            if offsets[i] < offsets[i - 1] {
                return Err(Error::OutOfSpec(format!(
                    "BinaryColumn offsets value must be equal or greater than previous value, but got {}",
                    offsets[i]
                )));
            }
        }
        Ok(())
    }
}

impl From<BinaryColumn> for ArrayData {
    fn from(column: BinaryColumn) -> Self {
        let builder = ArrayDataBuilder::new(DataType::LargeBinary)
            .len(column.len())
            .buffers(vec![column.offsets.into(), column.data.into()]);

        unsafe { builder.build_unchecked() }
    }
}

impl From<ArrayData> for BinaryColumn {
    fn from(data: ArrayData) -> Self {
        let offsets = data.buffers()[0].clone();
        let values = data.buffers()[1].clone();

        BinaryColumn::new(values.into(), offsets.into())
    }
}

impl<P: AsRef<[u8]>> FromIterator<P> for BinaryColumn {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        BinaryColumnBuilder::from_iter(iter).into()
    }
}
