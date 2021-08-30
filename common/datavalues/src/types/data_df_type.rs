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

use common_arrow::arrow::types::NativeType;
use common_io::prelude::*;
use num::NumCast;

use super::data_type::*;
use crate::DFTryFrom;
use crate::DataField;
use crate::DataValue;

pub trait DFDataType: std::fmt::Debug + Send + 'static + Sync {
    fn data_type() -> DataType;
}

macro_rules! impl_df_datatype {
    ($ca:ident, $variant:ident) => {
        impl DFDataType for $ca {
            fn data_type() -> DataType {
                DataType::$variant
            }
        }
    };
}

impl_df_datatype!(u8, UInt8);
impl_df_datatype!(u16, UInt16);
impl_df_datatype!(u32, UInt32);
impl_df_datatype!(u64, UInt64);

impl_df_datatype!(i8, Int8);
impl_df_datatype!(i16, Int16);
impl_df_datatype!(i32, Int32);
impl_df_datatype!(i64, Int64);
impl_df_datatype!(f32, Float32);
impl_df_datatype!(f64, Float64);
impl_df_datatype!(bool, Boolean);

impl_df_datatype!(String, Utf8);

#[derive(Debug)]
pub struct Null;
impl_df_datatype!(Null, Null);

impl DFDataType for Vec<u8> {
    fn data_type() -> DataType {
        DataType::Binary
    }
}

#[derive(Debug)]
pub struct List;
impl DFDataType for List {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::List(Box::new(DataField::new("", DataType::Null, true)))
    }
}

#[derive(Debug)]
pub struct Struct;
impl DFDataType for Struct {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::Struct(vec![DataField::new("", DataType::Null, true)])
    }
}

pub trait DFPrimitiveType:
    DFDataType
    + NativeType
    + NumCast
    + PartialOrd
    + Into<DataValue>
    + Default
    + BinarySer
    + BinaryDe
    + DFTryFrom<DataValue>
{
    type LargestType: DFPrimitiveType;
    const SIGN: bool;
    const FLOATING: bool;
    const SIZE: usize;
}

macro_rules! impl_primitive {
    ($ca:ident, $lg: ident, $sign: expr, $floating: expr, $size: expr) => {
        impl DFPrimitiveType for $ca {
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

pub trait DFIntegerType: DFPrimitiveType {}

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

pub trait DFFloatType: DFPrimitiveType {}
impl DFFloatType for f32 {}
impl DFFloatType for f64 {}
