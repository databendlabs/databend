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
use common_exception::ErrorCode;
use common_exception::Result;
use num::NumCast;
use primitive_types::U256;
use primitive_types::U512;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::DFTryFrom;
use crate::DataTypeImpl;
use crate::DataValue;
use crate::DateType;
use crate::Scalar;
use crate::TimestampType;
use crate::TypeID;
use crate::VariantType;
use crate::VariantValue;

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

pub trait LogicalDateType: PrimitiveType {
    fn get_type_id() -> TypeID;
}
impl LogicalDateType for i32 {
    fn get_type_id() -> TypeID {
        TypeID::Date
    }
}
impl LogicalDateType for i64 {
    fn get_type_id() -> TypeID {
        TypeID::Timestamp
    }
}

pub trait ToDateType {
    fn to_date_type() -> DataTypeImpl;
}

impl ToDateType for i32 {
    fn to_date_type() -> DataTypeImpl {
        DateType::new_impl()
    }
}

impl ToDateType for i64 {
    fn to_date_type() -> DataTypeImpl {
        TimestampType::new_impl(6)
    }
}

pub trait ObjectType:
    std::fmt::Display
    + Clone
    + std::marker::Sync
    + std::marker::Send
    + DFTryFrom<DataValue>
    + Into<DataValue>
    + core::str::FromStr
    + DeserializeOwned
    + Serialize
    + Default
    + Scalar
{
    fn data_type() -> DataTypeImpl;

    fn column_name() -> &'static str;

    fn memory_size(&self) -> usize;
}

impl ObjectType for VariantValue {
    fn data_type() -> DataTypeImpl {
        VariantType::new_impl()
    }

    fn column_name() -> &'static str {
        "VariantColumn"
    }

    fn memory_size(&self) -> usize {
        self.calculate_memory_size()
    }
}

pub trait LargePrimitive: Default + Sized + 'static {
    const BYTE_SIZE: usize;
    fn serialize_to(&self, _bytes: &mut [u8]);
    fn from_bytes(v: &[u8]) -> Result<Self>;
}

impl LargePrimitive for u128 {
    const BYTE_SIZE: usize = 16;
    fn serialize_to(&self, bytes: &mut [u8]) {
        let bs = self.to_le_bytes();
        bytes.copy_from_slice(&bs);
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        let bs: [u8; 16] = v.try_into().map_err(|_| {
            ErrorCode::StrParseError(format!(
                "Unable to parse into u128, unexpected byte size: {}",
                v.len()
            ))
        })?;
        Ok(u128::from_le_bytes(bs))
    }
}

impl LargePrimitive for U256 {
    const BYTE_SIZE: usize = 32;
    fn serialize_to(&self, bytes: &mut [u8]) {
        self.to_little_endian(bytes);
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        Ok(U256::from_little_endian(v))
    }
}

impl LargePrimitive for U512 {
    const BYTE_SIZE: usize = 64;
    fn serialize_to(&self, bytes: &mut [u8]) {
        self.to_little_endian(bytes);
    }

    fn from_bytes(v: &[u8]) -> Result<Self> {
        Ok(U512::from_little_endian(v))
    }
}
