// Copyright 2020 Datafuse Labs.
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

#[macro_export]
macro_rules! with_match_primitive_type {
    (
    $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }
        use common_datavalues::prelude::DataType::*;

        match $key_type {
            Int8 => __with_ty__! { i8 },
            Int16 => __with_ty__! { i16 },
            Int32 => __with_ty__! { i32 },
            Int64 => __with_ty__! { i64 },
            UInt8 => __with_ty__! { u8 },
            UInt16 => __with_ty__! { u16 },
            UInt32 => __with_ty__! { u32 },
            UInt64 => __with_ty__! { u64 },
            Float32 => __with_ty__! { f32 },
            Float64 => __with_ty__! { f64 },

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! dispatch_unsigned_numeric_types {
    ($dispatch: ident, $data_type: expr,  $($args:expr),*) => {
        $dispatch! { u8, $data_type,      $($args),* }
        $dispatch! { u16, $data_type,     $($args),* }
        $dispatch! { u32, $data_type,     $($args),* }
        $dispatch! { u64, $data_type,     $($args),* }
    };
}
