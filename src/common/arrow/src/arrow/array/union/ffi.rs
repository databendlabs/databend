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
use super::UnionArray;
use crate::arrow::array::FromFfi;
use crate::arrow::error::Result;
use crate::arrow::ffi;

unsafe impl ToFfi for UnionArray {
    fn buffers(&self) -> Vec<Option<*const u8>> {
        if let Some(offsets) = &self.offsets {
            vec![
                Some(self.types.as_ptr().cast::<u8>()),
                Some(offsets.as_ptr().cast::<u8>()),
            ]
        } else {
            vec![Some(self.types.as_ptr().cast::<u8>())]
        }
    }

    fn children(&self) -> Vec<Box<dyn Array>> {
        self.fields.clone()
    }

    fn offset(&self) -> Option<usize> {
        Some(self.types.offset())
    }

    fn to_ffi_aligned(&self) -> Self {
        self.clone()
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for UnionArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();
        let fields = Self::get_fields(&data_type);

        let mut types = unsafe { array.buffer::<i8>(0) }?;
        let offsets = if Self::is_sparse(&data_type) {
            None
        } else {
            Some(unsafe { array.buffer::<i32>(1) }?)
        };

        let length = array.array().len();
        let offset = array.array().offset();
        let fields = (0..fields.len())
            .map(|index| {
                let child = array.child(index)?;
                ffi::try_from(child)
            })
            .collect::<Result<Vec<Box<dyn Array>>>>()?;

        if offset > 0 {
            types.slice(offset, length);
        };

        Self::try_new(data_type, types, fields, offsets)
    }
}
