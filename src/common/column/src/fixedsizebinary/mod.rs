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
pub use builder::FixedSizeBinaryColumnBuilder;
pub use iterator::FixedSizeBinaryColumnBuilderIter;
pub use iterator::FixedSizeBinaryColumnIter;

use crate::bitmap::utils::BitmapIter;
use crate::bitmap::utils::ZipValidity;
use crate::bitmap::Bitmap;
use crate::buffer::Buffer;
use crate::error::Error;
use crate::error::Result;

#[derive(Clone, PartialEq)]
pub struct FixedSizeBinaryColumn {
    pub(crate) data: Buffer<u8>,
    pub(crate) value_length: usize,
}

impl FixedSizeBinaryColumn {
    pub fn new(data: Buffer<u8>, value_length: usize) -> Self {
        FixedSizeBinaryColumn { data, value_length }
    }

    pub fn len(&self) -> usize {
        self.data.len() / self.value_length
    }

    pub fn value_length(&self) -> usize {
        self.value_length
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_bytes_len(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &Buffer<u8> {
        &self.data
    }

    pub fn memory_size(&self) -> usize {
        self.total_bytes_len()
    }

    pub fn index(&self, index: usize) -> Option<&[u8]> {
        if index < self.len() {
            Some(&self.data[index..index + self.value_length])
        } else {
            None
        }
    }

    pub fn value(&self, index: usize) -> &[u8] {
        assert!(index + 1 < self.len());
        &self.data[index..index + self.value_length]
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &[u8] {
        self.data
            .get_unchecked(index * self.value_length..(index + 1) * self.value_length)
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        FixedSizeBinaryColumn {
            data: self
                .data
                .clone()
                .sliced(range.start, (range.end - range.start) * self.value_length),
            value_length: self.value_length,
        }
    }

    pub fn iter(&self) -> FixedSizeBinaryColumnIter {
        FixedSizeBinaryColumnIter::new(self)
    }

    pub fn option_iter<'a>(
        &'a self,
        validity: Option<&'a Bitmap>,
    ) -> ZipValidity<&'a [u8], FixedSizeBinaryColumnIter<'a>, BitmapIter<'a>> {
        let bitmap_iter = validity.as_ref().map(|v| v.iter());
        ZipValidity::new(self.iter(), bitmap_iter)
    }

    pub fn into_buffer(self) -> (Buffer<u8>, usize) {
        (self.data, self.value_length)
    }

    pub fn check_valid(&self) -> Result<()> {
        if self.is_empty() {
            return Err(Error::OutOfSpec(format!(
                "FixedSizeBinaryColumn length must be equal or greater than 1, but got {}",
                self.len()
            )));
        }

        if self.data.len() % self.value_length != 0 {
            return Err(Error::OutOfSpec(
                "FixedSizeBinaryColumn value must be equal with previous value".to_string(),
            ));
        }

        Ok(())
    }
}

impl From<FixedSizeBinaryColumn> for ArrayData {
    fn from(column: FixedSizeBinaryColumn) -> Self {
        let builder = ArrayDataBuilder::new(DataType::FixedSizeBinary(column.value_length as i32))
            .len(column.len())
            .buffers(vec![column.data.into()]);

        unsafe { builder.build_unchecked() }
    }
}

impl From<ArrayData> for FixedSizeBinaryColumn {
    fn from(data: ArrayData) -> Self {
        match data.data_type() {
            DataType::FixedSizeBinary(value_length) => {
                let values = data.buffers()[0].clone();
                FixedSizeBinaryColumn::new(values.into(), *value_length as usize)
            }
            _ => {
                panic!("Can only convert FixedSizeBinary Arrow array to FixedSizeBinaryColumn");
            }
        }
    }
}

impl<P: AsRef<[u8]>> FromIterator<P> for FixedSizeBinaryColumn {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        FixedSizeBinaryColumnBuilder::from_iter(iter).into()
    }
}
