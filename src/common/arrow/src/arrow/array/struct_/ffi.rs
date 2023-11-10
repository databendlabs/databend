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
use super::super::FromFfi;
use super::StructArray;
use crate::arrow::error::Result;
use crate::arrow::ffi;

unsafe impl ToFfi for StructArray {
    fn buffers(&self) -> Vec<Option<*const u8>> {
        vec![self.validity.as_ref().map(|x| x.as_ptr())]
    }

    fn children(&self) -> Vec<Box<dyn Array>> {
        self.values.clone()
    }

    fn offset(&self) -> Option<usize> {
        Some(
            self.validity
                .as_ref()
                .map(|bitmap| bitmap.offset())
                .unwrap_or_default(),
        )
    }

    fn to_ffi_aligned(&self) -> Self {
        self.clone()
    }
}

impl<A: ffi::ArrowArrayRef> FromFfi<A> for StructArray {
    unsafe fn try_from_ffi(array: A) -> Result<Self> {
        let data_type = array.data_type().clone();
        let fields = Self::get_fields(&data_type);

        let arrow_array = array.array();
        let validity = unsafe { array.validity() }?;
        let len = arrow_array.len();
        let offset = arrow_array.offset();
        let values = (0..fields.len())
            .map(|index| {
                let child = array.child(index)?;
                ffi::try_from(child).map(|arr| {
                    // there is a discrepancy with how arrow2 exports sliced
                    // struct array and how pyarrow does it.
                    // # Pyarrow
                    // ## struct array len 3
                    //  * slice 1 by with len 2
                    //      offset on struct array: 1
                    //      length on struct array: 2
                    //      offset on value array: 0
                    //      length on value array: 3
                    // # Arrow2
                    // ## struct array len 3
                    //  * slice 1 by with len 2
                    //      offset on struct array: 0
                    //      length on struct array: 3
                    //      offset on value array: 1
                    //      length on value array: 2
                    //
                    // this branch will ensure both can round trip
                    if arr.len() >= (len + offset) {
                        arr.sliced(offset, len)
                    } else {
                        arr
                    }
                })
            })
            .collect::<Result<Vec<Box<dyn Array>>>>()?;

        Self::try_new(data_type, values, validity)
    }
}
