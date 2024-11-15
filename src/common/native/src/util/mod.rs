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

mod bit_util;
mod byte_writer;
#[allow(dead_code)]
pub mod env;
pub mod memory;

use arrow_schema::DataType;
pub use bit_util::*;
pub use byte_writer::ByteWriter;

#[macro_export]
macro_rules! with_match_integer_double_type {
    (
    $key_type:expr, | $_:tt $I:ident | $body_integer:tt, | $__ :tt $T:ident | $body_primitive:tt
) => {{
        macro_rules! __with_ty__ {
            ( $_ $I:ident ) => {
                $body_integer
            };
        }
        macro_rules! __with_ty_double__ {
            ( $_ $T:ident ) => {
                $body_primitive
            };
        }
        use arrow_buffer::i256;
        use arrow_schema::DataType::*;
        match $key_type {
            Int8 => __with_ty__! { i8 },
            Int16 => __with_ty__! { i16 },
            Int32 => __with_ty__! { i32 },
            Int64 => __with_ty__! { i64 },
            Int128 => __with_ty__! { i128 },
            Int256 => __with_ty__! { i256 },
            UInt8 => __with_ty__! { u8 },
            UInt16 => __with_ty__! { u16 },
            UInt32 => __with_ty__! { u32 },
            UInt64 => __with_ty__! { u64 },

            Date32 => __with_ty__! { i32 },
            Date64 => __with_ty__! { i64 },
            Timestamp(_, _) => __with_ty__! { i64 },

            Float32 => __with_ty_double__! { f32 },
            Float64 => __with_ty_double__! { f64 },
            Float16 => unreachable! {},
            _ => unimplemented!(),
        }
    }};
}

#[macro_export]
macro_rules! with_match_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}

    use arrow_buffer::i256;
    use arrow_schema::DataType::*;

    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        Int256 => __with_ty__! { i256 },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },

        Date32 => __with_ty__! { i32 },
        Date64 => __with_ty__! { i64 },
        Timestamp(_, _) => __with_ty__! { i64 },
        _ => panic!("Do not support primitive `{:?}`", $key_type)
    }
})}

/// Returns the number of (parquet) columns that a [`DataType`] contains.
pub fn n_columns(data_type: &DataType) -> usize {
    use arrow_schema::DataType::*;
    match data_type {
        Null | Boolean | Binary | FixedSizeBinary(_) | LargeBinary | Utf8 | LargeUtf8
        | BinaryView | Utf8View => 1,

        List(_) | FixedSizeList(_, _) | LargeList(_) => {
            if let DataType::List(inner) = data_type {
                n_columns(inner.data_type())
            } else if let DataType::LargeList(inner) = data_type {
                n_columns(inner.data_type())
            } else if let DataType::FixedSizeList(inner, _) = data_type {
                n_columns(inner.data_type())
            } else {
                unreachable!()
            }
        }
        Map(inner, _) => n_columns(inner.data_type()),
        Struct(fields) => fields
            .iter()
            .map(|inner| n_columns(inner.data_type()))
            .sum(),
        other if other.is_primitive() => 1,
        other => unimplemented!("{:?}", other),
    }
}
