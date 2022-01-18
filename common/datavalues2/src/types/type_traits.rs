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

use crate::prelude::*;
use crate::Column;
use crate::DataType;
use crate::DataValue;

pub trait PrimitiveType:
    NativeArithmetics + NumCast + PartialOrd + Into<DataValue> + Default + Serialize + DeserializeOwned
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

pub trait DFIntegerType: PrimitiveType {}

macro_rules! impl_integer {
    ($ca:ident, $native:ident) => {
        impl DFIntegerType for $ca {}
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

pub trait ScalarType: Sized {
    type Type: DataType;
    type ColumnType: Column + 'static;
    type MutableColumnType: MutableColumn<Self, Self::ColumnType>;
}

macro_rules! impl_primitive_scalar_type {
    ($native:ident) => {
        impl ScalarType for $native {
            type Type = PrimitiveDataType<$native>;
            type ColumnType = PrimitiveColumn<$native>;
            type MutableColumnType = MutablePrimitiveColumn<$native>;
        }
    };
}

impl_primitive_scalar_type!(u8);
impl_primitive_scalar_type!(u16);
impl_primitive_scalar_type!(u32);
impl_primitive_scalar_type!(u64);
impl_primitive_scalar_type!(i8);
impl_primitive_scalar_type!(i16);
impl_primitive_scalar_type!(i32);
impl_primitive_scalar_type!(i64);
impl_primitive_scalar_type!(f32);
impl_primitive_scalar_type!(f64);

impl ScalarType for bool {
    type Type = BooleanType;
    type ColumnType = BooleanColumn;
    type MutableColumnType = MutableBooleanColumn;
}

impl ScalarType for &[u8] {
    type Type = StringType;
    type ColumnType = StringColumn;
    type MutableColumnType = MutableStringColumn;
}
