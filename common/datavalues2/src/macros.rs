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
    use crate::PrimitiveTypeID::*;
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
            TypeID::String => __with_ty__! { Vu8 },
            TypeID::Boolean => __with_ty__! { bool },

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_scalar_types_error {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use common_datavalues2::PhysicalTypeID::*;
    use common_datavalues2::PrimitiveTypeID;
    use common_exception::ErrorCode;
    type C = Vec<u8>;
    match $key_type {
        PhysicalTypeID::Boolean => __with_ty__! { bool },
       	PhysicalTypeID::String => __with_ty__! { C },

       	Primitive(PrimitiveTypeID::Int8) => __with_ty__! { i8 },
        Primitive(PrimitiveTypeID::Int16) => __with_ty__! { i16 },
        Primitive(PrimitiveTypeID::Int32) => __with_ty__! { i32 },
        Primitive(PrimitiveTypeID::Int64) => __with_ty__! { i64 },
        Primitive(PrimitiveTypeID::UInt8) => __with_ty__! { u8 },
        Primitive(PrimitiveTypeID::UInt16) => __with_ty__! { u16 },
        Primitive(PrimitiveTypeID::UInt32) => __with_ty__! { u32 },
        Primitive(PrimitiveTypeID::UInt64) => __with_ty__! { u64 },
        Primitive(PrimitiveTypeID::Float32) => __with_ty__! { f32 },
        Primitive(PrimitiveTypeID::Float64) => __with_ty__! { f64 },
        v => return Err(ErrorCode::BadDataValueType(
            format!("Ops is not support on datatype: {:?}",v)
        ))
    }
})}

#[macro_export]
macro_rules! with_match_primitive_type_id {
    (
    $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
) => {{
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
macro_rules! with_match_date_type_error {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt
    ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            TypeID::Date16 => __with_ty__! { u16},
            TypeID::Date32 => __with_ty__! { i32},
            TypeID::DateTime32 => __with_ty__! { u32},
            TypeID::DateTime64 => __with_ty__! { u64},
            v => Err(ErrorCode::BadDataValueType(format!(
                "Ops is not support on datatype: {:?}",
                v
            ))),
        }
    }};
}

// doesn't include Bool and String
#[macro_export]
macro_rules! apply_method_numeric_series {
    ($self:ident, $method:ident, $($args:expr),*) => {
        match $self.data_type() {
            TypeID::UInt8 => $self.u8().unwrap().$method($($args),*),
            TypeID::UInt16 => $self.u16().unwrap().$method($($args),*),
            TypeID::UInt32 => $self.u32().unwrap().$method($($args),*),
            TypeID::UInt64 => $self.u64().unwrap().$method($($args),*),
            TypeID::Int8 => $self.i8().unwrap().$method($($args),*),
            TypeID::Int16 => $self.i16().unwrap().$method($($args),*),
            TypeID::Int32 => $self.i32().unwrap().$method($($args),*),
            TypeID::Int64 => $self.i64().unwrap().$method($($args),*),
            TypeID::Float32 => $self.f32().unwrap().$method($($args),*),
            TypeID::Float64 => $self.f64().unwrap().$method($($args),*),
            TypeID::Date16 => $self.u16().unwrap().$method($($args),*),
            TypeID::Date32 => $self.i32().unwrap().$method($($args),*),

            _ => unimplemented!(),
        }
    }
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

#[macro_export]
macro_rules! match_type_id_apply_macro {
    ($obj:expr, $macro:ident, $macro_string:ident, $macro_bool:ident $(, $opt_args:expr)*) => {{
        match $obj {
            TypeID::String => $macro_string!($($opt_args)*),
            TypeID::Boolean => $macro_bool!($($opt_args)*),
            TypeID::UInt8 => $macro!(u8 $(, $opt_args)*),
            TypeID::UInt16 => $macro!(u16 $(, $opt_args)*),
            TypeID::UInt32 => $macro!(u32 $(, $opt_args)*),
            TypeID::UInt64 => $macro!(u64 $(, $opt_args)*),
            TypeID::Int8 => $macro!(i8 $(, $opt_args)*),
            TypeID::Int16 => $macro!(i16 $(, $opt_args)*),
            TypeID::Int32 => $macro!(i32 $(, $opt_args)*),
            TypeID::Int64 => $macro!(i64 $(, $opt_args)*),
            TypeID::Float32 => $macro!(f32 $(, $opt_args)*),
            TypeID::Float64 => $macro!(f64 $(, $opt_args)*),
            _ => unimplemented!(),
        }
    }};
}
