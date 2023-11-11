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
use crate::match_integer_type;
use crate::with_match_primitive_type;

macro_rules! ffi_dyn {
    ($array:expr, $ty:ty) => {{
        let a = $array.as_any().downcast_ref::<$ty>().unwrap();
        if a.offset().is_some() {
            $array
        } else {
            Box::new(a.to_ffi_aligned())
        }
    }};
}

pub fn align_to_c_data_interface(array: Box<dyn Array>) -> Box<dyn Array> {
    use crate::arrow::datatypes::PhysicalType::*;
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
                ffi_dyn!(array, DictionaryArray<$T>)
            })
        }
    }
}
