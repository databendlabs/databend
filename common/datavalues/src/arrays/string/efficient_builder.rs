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

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::MutableBuffer;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::error::ArrowError;
use common_arrow::arrow::types::Index;

use crate::prelude::*;

pub struct EfficientStringArrayBuilder {
    data_type: DataType,
    offsets: MutableBuffer<i64>,
    values: MutableBuffer<u8>,
    validity: Option<MutableBitmap>,
}

impl EfficientStringArrayBuilder {
    pub fn with_capacity(capacity: usize, bytes_size: usize) -> Self {
        let mut offsets = MutableBuffer::<i64>::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            data_type: BinaryArray::<i64>::default_data_type(),
            offsets,
            values: MutableBuffer::<u8>::with_capacity(bytes_size),
            validity: None,
        }
    }

    /// Write values directly into the underlying data.
    /// # Safety
    /// Caller must ensure that `len <= self.capacity()`
    #[inline]
    pub fn write_values<F>(&mut self, mut f: F)
    where F: FnMut(&mut [u8]) -> usize {
        unsafe {
            self.values.set_len(self.values.capacity());
            let buffer = &mut self.values.as_mut_slice()[self.offsets.last().unwrap().to_usize()..];
            let len = f(buffer);
            let new_len = self.offsets.last().unwrap().to_usize() + len;
            self.values.set_len(new_len);

            let size = i64::from_usize(new_len)
                .ok_or(ArrowError::Overflow)
                .unwrap();
            self.offsets.push(size);
            match &mut self.validity {
                Some(validity) => validity.push(true),
                None => {}
            }
        }
    }

    #[inline]
    fn last_offset(&self) -> i64 {
        *self.offsets.last().unwrap()
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.offsets.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }

    pub fn write_null(&mut self) {
        self.offsets.push(self.last_offset());
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    pub fn finish(self) -> DFStringArray {
        let array: BinaryArray<i64> = self.into();
        DFStringArray::from_arrow_array(&array)
    }
}

impl From<EfficientStringArrayBuilder> for BinaryArray<i64> {
    fn from(other: EfficientStringArrayBuilder) -> Self {
        BinaryArray::<i64>::from_data(
            other.data_type,
            other.offsets.into(),
            other.values.into(),
            other.validity.map(|x| x.into()),
        )
    }
}
