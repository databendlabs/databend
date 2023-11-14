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

use super::PrimitiveArray;
use crate::arrow::array::FromFfi;
use crate::arrow::array::ToFfi;
use crate::arrow::bitmap::align;
use crate::arrow::error::Result;
use crate::arrow::ffi;
use crate::arrow::types::NativeType;

unsafe impl<T: NativeType> ToFfi for PrimitiveArray<T> {
    fn buffers(&self) -> Vec<Option<*const u8>> {
        vec![
            self.validity.as_ref().map(|x| x.as_ptr()),
            Some(self.values.as_ptr().cast::<u8>()),
        ]
    }

    fn offset(&self) -> Option<usize> {
        let offset = self.values.offset();
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
        let offset = self.values.offset();

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
            values: self.values.clone(),
        }
    }
}

impl<T: NativeType, A: ffi::ArrowArrayRef> FromFfi<A> for PrimitiveArray<T> {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();
        let validity = unsafe { array.validity() }?;
        let values = unsafe { array.buffer::<T>(1) }?;

        Self::try_new(data_type, values, validity)
    }
}
