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
        use $crate::arrow::datatypes::PrimitiveType::*;
        use $crate::arrow::types::i256;
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

            Float32 => __with_ty_double__! { f32 },
            Float64 => __with_ty_double__! { f64 },
            Float16 => unreachable! {},
            DaysMs => unreachable!(),
            MonthDayNano => unreachable!(),
            UInt128 => unimplemented!(),
        }
    }};
}
