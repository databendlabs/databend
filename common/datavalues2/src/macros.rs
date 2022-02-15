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
macro_rules! for_all_scalar_types {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 },
            { f32 },
            { f64 },
            { bool },
            { Vu8 }
        }
    };
}

#[macro_export]
macro_rules! for_all_primitive_types{
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 },
            { f32 },
            { f64 }
        }
    };
}

#[macro_export]
macro_rules! for_all_primitive_boolean_types{
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 },
            { f32 },
            { f64 },
            { bool }
        }
    };
}

#[macro_export]
macro_rules! for_all_scalar_varints{
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8, Int8 },
            { i16, Int16 },
            { i32, Int32 },
            { i64, Int64 },
            { u8, UInt8 },
            { u16, UInt16 },
            { u32, UInt32 },
            { u64, UInt64 },
            { f32, Float32 },
            { f64, Float64 },
            { bool, Boolean },
            { Vu8, String }
        }
    };
}

#[macro_export]
macro_rules! for_all_integer_types{
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 }
        }
    };
}

#[macro_export]
macro_rules! with_match_physical_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    match $key_type {
        PhysicalTypeID::Int8 => __with_ty__! { i8 },
        PhysicalTypeID::Int16 => __with_ty__! { i16 },
        PhysicalTypeID::Int32 => __with_ty__! { i32 },
        PhysicalTypeID::Int64 => __with_ty__! { i64 },
        PhysicalTypeID::UInt8 => __with_ty__! { u8 },
        PhysicalTypeID::UInt16 => __with_ty__! { u16 },
        PhysicalTypeID::UInt32 => __with_ty__! { u32 },
        PhysicalTypeID::UInt64 => __with_ty__! { u64 },
        PhysicalTypeID::Float32 => __with_ty__! { f32 },
        PhysicalTypeID::Float64 => __with_ty__! { f64 },
        _ => unreachable!()
    }
})}

#[macro_export]
macro_rules! with_match_scalar_type {
    (
    $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        type C = Vec<u8>;

        match $key_type {
            PhysicalTypeID::Boolean => __with_ty__! { bool },
            PhysicalTypeID::String => __with_ty__! { C },

            PhysicalTypeID::Int8 => __with_ty__! { i8 },
            PhysicalTypeID::Int16 => __with_ty__! { i16 },
            PhysicalTypeID::Int32 => __with_ty__! { i32 },
            PhysicalTypeID::Int64 => __with_ty__! { i64 },
            PhysicalTypeID::UInt8 => __with_ty__! { u8 },
            PhysicalTypeID::UInt16 => __with_ty__! { u16 },
            PhysicalTypeID::UInt32 => __with_ty__! { u32 },
            PhysicalTypeID::UInt64 => __with_ty__! { u64 },
            PhysicalTypeID::Float32 => __with_ty__! { f32 },
            PhysicalTypeID::Float64 => __with_ty__! { f64 },

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_scalar_types_error {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use common_exception::ErrorCode;
    type C = Vec<u8>;
    match $key_type {
        PhysicalTypeID::Boolean => __with_ty__! { bool },
       	PhysicalTypeID::String => __with_ty__! { C },

       	PhysicalTypeID::Int8 => __with_ty__! { i8 },
        PhysicalTypeID::Int16 => __with_ty__! { i16 },
        PhysicalTypeID::Int32 => __with_ty__! { i32 },
        PhysicalTypeID::Int64 => __with_ty__! { i64 },
        PhysicalTypeID::UInt8 => __with_ty__! { u8 },
        PhysicalTypeID::UInt16 => __with_ty__! { u16 },
        PhysicalTypeID::UInt32 => __with_ty__! { u32 },
        PhysicalTypeID::UInt64 => __with_ty__! { u64 },
        PhysicalTypeID::Float32 => __with_ty__! { f32 },
        PhysicalTypeID::Float64 => __with_ty__! { f64 },
        v => return Err(ErrorCode::BadDataValueType(
            format!("Ops is not support on datatype: {:?}",v)
        ))
    }
})}

#[macro_export]
macro_rules! with_match_primitive_type_id {
    ($key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            TypeID::Int8 => __with_ty__! { i8 },
            TypeID::Int16 => __with_ty__! { i16 },
            TypeID::Int32 => __with_ty__! { i32 },
            TypeID::Int64 => __with_ty__! { i64 },
            TypeID::UInt8 => __with_ty__! { u8 },
            TypeID::UInt16 => __with_ty__! { u16 },
            TypeID::UInt32 => __with_ty__! { u32 },
            TypeID::UInt64 => __with_ty__! { u64 },
            TypeID::Float32 => __with_ty__! { f32 },
            TypeID::Float64 => __with_ty__! { f64 },

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_primitive_types_error {
    ($key_type:expr, | $_:tt $T:ident | $body:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            TypeID::Int8 => __with_ty__! { i8 },
            TypeID::Int16 => __with_ty__! { i16 },
            TypeID::Int32 => __with_ty__! { i32 },
            TypeID::Int64 => __with_ty__! { i64 },
            TypeID::UInt8 => __with_ty__! { u8 },
            TypeID::UInt16 => __with_ty__! { u16 },
            TypeID::UInt32 => __with_ty__! { u32 },
            TypeID::UInt64 => __with_ty__! { u64 },
            TypeID::Float32 => __with_ty__! { f32 },
            TypeID::Float64 => __with_ty__! { f64 },
            v => Err(ErrorCode::BadDataValueType(format!(
                "Ops is not support on datatype: {:?}",
                v
            ))),
        }
    }};
}

#[macro_export]
macro_rules! with_match_date_type_error {
    ($key_type:expr, | $_:tt $T:ident | $body:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            TypeID::Date16 => __with_ty__! { u16},
            TypeID::Date32 => __with_ty__! { i32},
            TypeID::DateTime32 => __with_ty__! { u32},
            TypeID::DateTime64 => __with_ty__! { i64},
            v => Err(ErrorCode::BadDataValueType(format!(
                "Ops is not support on datatype: {:?}",
                v
            ))),
        }
    }};
}

macro_rules! try_cast_data_value_to_std {
    ($NATIVE: ident, $AS_FN: ident) => {
        impl DFTryFrom<DataValue> for $NATIVE {
            fn try_from(value: DataValue) -> Result<Self> {
                let v = value.$AS_FN()?;
                Ok(v as $NATIVE)
            }
        }

        impl DFTryFrom<&DataValue> for $NATIVE {
            fn try_from(value: &DataValue) -> Result<Self> {
                let v = value.$AS_FN()?;
                Ok(v as $NATIVE)
            }
        }
    };
}

macro_rules! std_to_data_value {
    ($SCALAR:ident, $NATIVE:ident, $UPPER: ident) => {
        impl From<$NATIVE> for DataValue {
            fn from(value: $NATIVE) -> Self {
                DataValue::$SCALAR(value as $UPPER)
            }
        }

        impl From<Option<$NATIVE>> for DataValue {
            fn from(value: Option<$NATIVE>) -> Self {
                match value {
                    Some(v) => DataValue::$SCALAR(v as $UPPER),
                    None => DataValue::Null,
                }
            }
        }
    };
}
