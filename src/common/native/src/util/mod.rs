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

#[allow(dead_code)]
mod bit_util;
mod byte_writer;
#[allow(dead_code)]
pub mod env;
pub mod memory;

pub use bit_util::*;
pub use byte_writer::ByteWriter;
use databend_common_expression::TableDataType;

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
        use databend_common_expression::types::F32;
        use databend_common_expression::types::F64;
        use databend_common_expression::types::NumberDataType::*;

        match $key_type {
            Int8 => __with_ty__! { i8 },
            Int16 => __with_ty__! { i16 },
            Int32 => __with_ty__! { i32 },
            Int64 => __with_ty__! { i64 },
            UInt8 => __with_ty__! { u8 },
            UInt16 => __with_ty__! { u16 },
            UInt32 => __with_ty__! { u32 },
            UInt64 => __with_ty__! { u64 },

            Float32 => __with_ty_double__! { F32 },
            Float64 => __with_ty_double__! { F64 },
        }
    }};
}

/// Returns the number of (parquet) columns that a [`DataType`] contains.
pub fn n_columns(data_type: &TableDataType) -> usize {
    use TableDataType::*;

    match data_type.remove_nullable() {
        Array(inner) => n_columns(&inner),
        Map(inner) => n_columns(&inner),
        Tuple { fields_type, .. } => fields_type.iter().map(n_columns).sum(),
        _ => 1,
    }
}
