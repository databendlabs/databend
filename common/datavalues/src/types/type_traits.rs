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

use common_arrow::arrow::compute::arithmetics::basic::NativeArithmetics;
use num::NumCast;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::DFTryFrom;
use crate::DataTypePtr;
use crate::DataValue;
use crate::Date16Type;
use crate::Date32Type;
use crate::DateTime32Type;
use crate::DateTime64Type;
use crate::Scalar;

pub trait PrimitiveType:
    NativeArithmetics
    + DFTryFrom<DataValue>
    + NumCast
    + PartialOrd
    + Into<DataValue>
    + Default
    + Serialize
    + DeserializeOwned
    + Scalar
{
    type LargestType: PrimitiveType;
    const SIGN: bool;
    const FLOATING: bool;
    const SIZE: usize;
}

macro_rules! impl_primitive {
    ($ca:ident, $lg: ident, $sign: expr, $floating: expr, $size: expr) => {
        impl PrimitiveType for $ca {
            type LargestType = $lg;
            const SIGN: bool = $sign;
            const FLOATING: bool = $floating;
            const SIZE: usize = $size;
        }
    };
}

impl_primitive!(u8, u64, false, false, 1);
impl_primitive!(u16, u64, false, false, 2);
impl_primitive!(u32, u64, false, false, 4);
impl_primitive!(u64, u64, false, false, 8);
impl_primitive!(i8, i64, true, false, 1);
impl_primitive!(i16, i64, true, false, 2);
impl_primitive!(i32, i64, true, false, 4);
impl_primitive!(i64, i64, true, false, 8);
impl_primitive!(f32, f64, true, true, 4);
impl_primitive!(f64, f64, true, true, 8);

pub trait IntegerType: PrimitiveType {}

macro_rules! impl_integer {
    ($ca:ident, $native:ident) => {
        impl IntegerType for $ca {}
    };
}

impl_integer!(u8, u8);
impl_integer!(u16, u16);
impl_integer!(u32, u32);
impl_integer!(u64, u64);
impl_integer!(i8, i8);
impl_integer!(i16, i16);
impl_integer!(i32, i32);
impl_integer!(i64, i64);

pub trait FloatType: PrimitiveType {}
impl FloatType for f32 {}
impl FloatType for f64 {}

pub trait DateType: PrimitiveType {}
impl DateType for u16 {}
impl DateType for i32 {}
impl DateType for u32 {}
impl DateType for i64 {}

pub trait ToDateType {
    fn to_date_type() -> DataTypePtr;
}

impl ToDateType for u16 {
    fn to_date_type() -> DataTypePtr {
        Date16Type::arc()
    }
}

impl ToDateType for i32 {
    fn to_date_type() -> DataTypePtr {
        Date32Type::arc()
    }
}

impl ToDateType for u32 {
    fn to_date_type() -> DataTypePtr {
        DateTime32Type::arc(None)
    }
}

impl ToDateType for i64 {
    fn to_date_type() -> DataTypePtr {
        DateTime64Type::arc(0, None)
    }
}
