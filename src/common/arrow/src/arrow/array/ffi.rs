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

use crate::arrow::array::*;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::arrow::ffi;

/// Trait describing how a struct presents itself to the
/// [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
/// # Safety
/// Implementing this trait incorrect will lead to UB
pub(crate) unsafe trait ToFfi {
    /// The pointers to the buffers.
    fn buffers(&self) -> Vec<Option<*const u8>>;

    /// The children
    fn children(&self) -> Vec<Box<dyn Array>> {
        vec![]
    }

    /// The offset
    fn offset(&self) -> Option<usize>;

    /// return a partial clone of self with an offset.
    fn to_ffi_aligned(&self) -> Self;
}

/// Trait describing how a struct imports into itself from the
/// [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html) (FFI).
pub(crate) trait FromFfi<T: ffi::ArrowArrayRef>: Sized {
    /// Convert itself from FFI.
    /// # Safety
    /// This function is intrinsically `unsafe` as it requires the FFI to be made according
    /// to the [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
    unsafe fn try_from_ffi(array: T) -> Result<Self>;
}

macro_rules! ffi_dyn {
    ($array:expr, $ty:ty) => {{
        let array = $array.as_any().downcast_ref::<$ty>().unwrap();
        (
            array.offset().unwrap(),
            array.buffers(),
            array.children(),
            None,
        )
    }};
}

type BuffersChildren = (
    usize,
    Vec<Option<*const u8>>,
    Vec<Box<dyn Array>>,
    Option<Box<dyn Array>>,
);

pub fn offset_buffers_children_dictionary(array: &dyn Array) -> BuffersChildren {
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => ffi_dyn!(array, NullArray),
        Boolean => ffi_dyn!(array, BooleanArray),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            ffi_dyn!(array, PrimitiveArray<$T>)
        }),
        Binary => ffi_dyn!(array, BinaryArray<i32>),
        LargeBinary => ffi_dyn!(array, BinaryArray<i64>),
        FixedSizeBinary => ffi_dyn!(array, FixedSizeBinaryArray),
        Utf8 => ffi_dyn!(array, Utf8Array::<i32>),
        LargeUtf8 => ffi_dyn!(array, Utf8Array::<i64>),
        List => ffi_dyn!(array, ListArray::<i32>),
        LargeList => ffi_dyn!(array, ListArray::<i64>),
        FixedSizeList => ffi_dyn!(array, FixedSizeListArray),
        Struct => ffi_dyn!(array, StructArray),
        Union => ffi_dyn!(array, UnionArray),
        Map => ffi_dyn!(array, MapArray),
        Dictionary(key_type) => {
            match_integer_type!(key_type, |$T| {
                let array = array.as_any().downcast_ref::<DictionaryArray<$T>>().unwrap();
                (
                    array.offset().unwrap(),
                    array.buffers(),
                    array.children(),
                    Some(array.values().clone()),
                )
            })
        }
    }
}
