// Copyright 2020-2022 Jorge C. Leitão
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

use super::super::ffi::ToFfi;
use super::super::Array;
use super::MapArray;
use crate::arrow::array::FromFfi;
use crate::arrow::bitmap::align;
use crate::arrow::error::Result;
use crate::arrow::ffi;
use crate::arrow::offset::OffsetsBuffer;

unsafe impl ToFfi for MapArray {
    fn buffers(&self) -> Vec<Option<*const u8>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.offsets.buffer().as_ptr().cast::<u8>()),
        ]
    }

    fn children(&self) -> Vec<Box<dyn Array>> {
        vec![self.field.clone()]
    }

    fn offset(&self) -> Option<usize> {
        let offset = self.offsets.buffer().offset();
        if let Some(bitmap) = self.validity.as_ref() {
            if bitmap.offset() == offset {
                Some(offset)
            } else {
                None
            }
        } else {
            Some(offset)
        }
    }

    fn to_ffi_aligned(&self) -> Self {
        let offset = self.offsets.buffer().offset();

        let validity = self.validity.as_ref().map(|bitmap| {
            if bitmap.offset() == offset {
                bitmap.clone()
            } else {
                align(bitmap, offset)
            }
        });

        Self {
            data_type: self.data_type.clone(),
            validity,
            offsets: self.offsets.clone(),
            field: self.field.clone(),
        }
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for MapArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();
        let validity = unsafe { array.validity() }?;
        let offsets = unsafe { array.buffer::<i32>(1) }?;
        let child = array.child(0)?;
        let values = ffi::try_from(child)?;

        // assumption that data from FFI is well constructed
        let offsets = unsafe { OffsetsBuffer::new_unchecked(offsets) };

        Self::try_new(data_type, offsets, values, validity)
    }
}
