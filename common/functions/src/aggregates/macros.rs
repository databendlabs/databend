// Copyright 2021 Datafuse Labs.
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
macro_rules! with_match_primitive_type_ids {
    (
    $type0:expr, $type1:expr, | $_a:tt $T0:ident, $_b:tt $T1:ident | $body:tt,  $nbody:tt
) => {{
        use common_datavalues::prelude::TypeID::*;

        macro_rules! __with_types__ {
            ( $_a $T0:ident, $_b $T1:ident ) => {
                $body
            };
        }

        macro_rules! __match_type__ {
            ($t:ident) => {
                match $type1 {
                    Int8 => __with_types__! { $t, i8 },
                    Int16 => __with_types__! { $t, i16 },
                    Int32 => __with_types__! { $t, i32 },
                    Int64 => __with_types__! { $t, i64 },
                    UInt8 => __with_types__! { $t, u8 },
                    UInt16 => __with_types__! { $t, u16 },
                    UInt32 => __with_types__! { $t, u32 },
                    UInt64 => __with_types__! { $t, u64 },
                    Float32 => __with_types__! { $t, f32 },
                    Float64 => __with_types__! { $t, f64 },
                    _ => $nbody,
                }
            };
        }

        match $type0 {
            Int8 => __match_type__! { i8 },
            Int16 => __match_type__! { i16 },
            Int32 => __match_type__! { i32 },
            Int64 => __match_type__! { i64 },
            UInt8 => __match_type__! { u8 },
            UInt16 => __match_type__! { u16 },
            UInt32 => __match_type__! { u32 },
            UInt64 => __match_type__! { u64 },
            Float32 => __match_type__! { f32 },
            Float64 => __match_type__! { f64 },
            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_date_date_time_types {
    ($dispatch: ident, $data_type: expr,  $($args:expr),*) => {
        match $data_type {
            TypeID::Date16 => {
                $dispatch! { u16,  $($args),* }
            },
            TypeID::Date32 => {
                $dispatch! { i32,  $($args),* }
            }
            TypeID::DateTime32 => {
                $dispatch! { u32,  $($args),* }
            },
            _ => {},
        }
    };
}

#[macro_export]
macro_rules! with_match_unsigned_numeric_types {
    ($dispatch: ident, $data_type: expr,  $($args:expr),*) => {

        match $data_type {
            TypeID::UInt8 => {
                $dispatch! { u8,  $($args),* }
            },
            TypeID::UInt16 => {
                $dispatch! { u16,  $($args),* }
            }
            TypeID::UInt32 => {
                $dispatch! { u32,  $($args),* }
            },
            TypeID::UInt64 => {
                $dispatch! { u64,  $($args),* }
            },
            _ => {},
        }
    };
}
