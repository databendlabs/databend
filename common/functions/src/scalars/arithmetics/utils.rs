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

use std::fmt::Display;

use common_datavalues::prelude::DataColumnWithField;
use common_exception::ErrorCode;
use common_exception::Result;
/*
pub fn assert_unary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 1 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have single arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}*/

pub fn assert_binary_arguments<D: Display>(name: D, actual: usize) -> Result<()> {
    if actual != 2 {
        return Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "{} expect to have two arguments, but got {}",
            name, actual
        )));
    }
    Ok(())
}

pub fn validate_input<'a>(
    col0: &'a DataColumnWithField,
    col1: &'a DataColumnWithField,
) -> (&'a DataColumnWithField, &'a DataColumnWithField) {
    if col0.data_type().is_integer() || col0.data_type().is_interval() {
        (col0, col1)
    } else {
        (col1, col0)
    }
}

#[macro_export]
macro_rules! with_match_integer_8 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::UInt8 => __with_ty__! { u8},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_16 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::Int16 => __with_ty__! { i16},
            DataType::UInt8 => __with_ty__! { u8},
            DataType::UInt16 => __with_ty__! { u16},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_32 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::Int16 => __with_ty__! { i16},
            DataType::Int32 => __with_ty__! { i32},
            DataType::UInt8 => __with_ty__! { u8},
            DataType::UInt16 => __with_ty__! { u16},
            DataType::UInt32 => __with_ty__! { u32},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_64 {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Int8 => __with_ty__! { i8},
            DataType::Int16 => __with_ty__! { i16},
            DataType::Int32 => __with_ty__! { i32},
            DataType::Int64 => __with_ty__! { i64},
            DataType::UInt8 => __with_ty__! { u8},
            DataType::UInt16 => __with_ty__! { u16},
            DataType::UInt32 => __with_ty__! { u32},
            DataType::UInt64 => __with_ty__! { u64},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! with_match_date_type {
    (
         $key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt
     ) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            DataType::Date16 => __with_ty__! { u16},
            DataType::Date32 => __with_ty__! { i32},
            DataType::DateTime32(_) => __with_ty__! { u32},

            _ => $nbody,
        }
    }};
}

#[macro_export]
macro_rules! binary_arithmetic_helper {
    ($lhs: ident, $rhs: ident, $T:ident, $R:ident, $op: expr) => {{
        match ($lhs.len(), $rhs.len()) {
            (a, b) if a == b => binary($lhs, $rhs, |l, r| $op(l.as_(), r.as_())),
            (_, 1) => {
                let opt_rhs = $rhs.get(0);
                match opt_rhs {
                    None => DFPrimitiveArray::<$R>::full_null($lhs.len()),
                    Some(rhs) => {
                        let r: $T = rhs.as_();
                        unary($lhs, |l| $op(l.as_(), r))
                    }
                }
            }
            (1, _) => {
                let opt_lhs = $lhs.get(0);
                match opt_lhs {
                    None => DFPrimitiveArray::<$R>::full_null($rhs.len()),
                    Some(lhs) => {
                        let l: $T = lhs.as_();
                        unary($rhs, |r| $op(l, r.as_()))
                    }
                }
            }
            _ => unreachable!(),
        }
        .into()
    }};
}

#[macro_export]
macro_rules! binary_arithmetic {
    ($self: expr, $rhs: expr, $R:ident, $op: expr) => {{
        let lhs = $self.to_minimal_array()?;
        let rhs = $rhs.to_minimal_array()?;
        let lhs: &DFPrimitiveArray<T> = lhs.static_cast();
        let rhs: &DFPrimitiveArray<D> = rhs.static_cast();

        let result: DataColumn = binary_arithmetic_helper!(lhs, rhs, $R, $R, $op);
        Ok(result.resize_constant($self.len()))
    }};
}

#[macro_export]
macro_rules! interval_arithmetic {
    ($self: expr, $rhs: expr, $R:ident, $op: expr) => {{
        let (interval, datetime) = validate_input($self, $rhs);
        let lhs = datetime.column().to_minimal_array()?;
        let lhs: &DFPrimitiveArray<$R> = lhs.static_cast();
        let rhs = interval.column().to_minimal_array()?;
        let rhs = rhs.i64()?;

        let result: DataColumn = binary_arithmetic_helper!(lhs, rhs, i64, $R, $op);
        Ok(result.resize_constant($self.column().len()))
    }};
}

#[macro_export]
macro_rules! arithmetic_helper {
    ($lhs: ident, $rhs: ident, $T: ident, $R: ident, $scalar: expr, $op: expr) => {{
        match ($lhs.len(), $rhs.len()) {
            (a, b) if a == b => binary($lhs, $rhs, |l, r| $op(l.as_(), r.as_())),
            (_, 1) => {
                let opt_rhs = $rhs.get(0);
                match opt_rhs {
                    None => DFPrimitiveArray::<$R>::full_null($lhs.len()),
                    Some(rhs) => {
                        let r: $T = rhs.as_();
                        $scalar($lhs, &r)
                    }
                }
            }
            (1, _) => {
                let opt_lhs = $lhs.get(0);
                match opt_lhs {
                    None => DFPrimitiveArray::<$R>::full_null($rhs.len()),
                    Some(lhs) => {
                        let l: $T = lhs.as_();
                        unary($rhs, |r| $op(l, r.as_()))
                    }
                }
            }
            _ => unreachable!(),
        }
        .into()
    }};
}
