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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::TryFromIntError;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Div;
use std::ops::DivAssign;
use std::ops::Mul;
use std::ops::MulAssign;
use std::ops::Neg;
use std::ops::Range;
use std::ops::Rem;
use std::ops::Sub;
use std::ops::SubAssign;

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::display_decimal_128;
use databend_common_io::display_decimal_256;
use enum_as_inner::EnumAsInner;
use ethnum::u256;
use ethnum::AsI256;
use itertools::Itertools;
use micromarshal::Marshal;
use num_bigint::BigInt;
use num_traits::FromBytes;
use num_traits::NumCast;
use num_traits::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::column_type_error;
use super::compute_view::Compute;
use super::compute_view::ComputeView;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::AnyType;
use super::DataType;
use super::NumberType;
use super::SimpleDomain;
use super::SimpleType;
use super::SimpleValueType;
use super::ValueType;
use super::F64;
use crate::utils::arrow::buffer_into_mut;
use crate::with_decimal_mapped_type;
use crate::with_decimal_type;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;
use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreDecimal<T: Decimal>(PhantomData<T>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreScalarDecimal<T: Decimal>(PhantomData<T>);

pub type Decimal64Type = DecimalType<i64>;
pub type Decimal128Type = DecimalType<i128>;
pub type Decimal256Type = DecimalType<i256>;

pub type DecimalType<T> = SimpleValueType<CoreDecimal<T>>;
pub type DecimalScalarType<T> = SimpleValueType<CoreScalarDecimal<T>>;

impl<Num: Decimal> AccessType for CoreDecimal<Num> {
    type Scalar = Num;
    type ScalarRef<'a> = Num;
    type Column = Buffer<Num>;
    type Domain = SimpleDomain<Num>;
    type ColumnIterator<'a> = std::iter::Copied<std::slice::Iter<'a, Num>>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let decimal = scalar
            .as_decimal()
            .ok_or_else(|| scalar_type_error::<Self>(scalar))?;
        Num::try_downcast_scalar(decimal).ok_or_else(|| scalar_type_error::<Self>(scalar))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        let decimal = domain
            .as_decimal()
            .ok_or_else(|| domain_type_error::<Self>(domain))?;
        Num::try_downcast_domain(decimal).ok_or_else(|| domain_type_error::<Self>(domain))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        Num::try_downcast_column(col)
            .map(|(col, _)| col)
            .ok_or_else(|| column_type_error::<Self>(col))
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index).copied()
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        *col.get_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter().copied()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(&rhs)
    }
}

impl<Num: Decimal> SimpleType for CoreDecimal<Num> {
    type Scalar = Num;
    type Domain = SimpleDomain<Num>;

    fn downcast_scalar(scalar: &ScalarRef) -> Option<Num> {
        Num::try_downcast_scalar(scalar.as_decimal()?)
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        Num::try_downcast_column(col).map(|(col, _)| col)
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        Num::try_downcast_domain(domain.as_decimal()?)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Num>> {
        Num::try_downcast_builder(builder)
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Num>> {
        Num::try_downcast_owned_builder(builder)
    }

    fn upcast_column_builder(builder: Vec<Num>, data_type: &DataType) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Decimal(Num::upcast_builder(
            builder,
            *data_type.as_decimal().unwrap(),
        )))
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        let size = *data_type.as_decimal().unwrap();
        Num::upcast_scalar(scalar, size)
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        let size = *data_type.as_decimal().unwrap();
        Num::upcast_column(col, size)
    }

    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain {
        let size = *data_type.as_decimal().unwrap();
        Num::upcast_domain(domain, size)
    }

    #[inline(always)]
    fn compare(lhs: &Num, rhs: &Num) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn greater_than(left: &Num, right: &Num) -> bool {
        left > right
    }

    #[inline(always)]
    fn greater_than_equal(left: &Num, right: &Num) -> bool {
        left >= right
    }

    #[inline(always)]
    fn less_than(left: &Num, right: &Num) -> bool {
        left < right
    }

    #[inline(always)]
    fn less_than_equal(left: &Num, right: &Num) -> bool {
        left <= right
    }
}

impl<Num: Decimal> AccessType for CoreScalarDecimal<Num> {
    type Scalar = (Num, DecimalSize);
    type ScalarRef<'a> = (Num, DecimalSize);
    type Column = Buffer<(Num, DecimalSize)>;
    type Domain = SimpleDomain<(Num, DecimalSize)>;
    type ColumnIterator<'a> = std::iter::Copied<std::slice::Iter<'a, (Num, DecimalSize)>>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let decimal = scalar
            .as_decimal()
            .ok_or_else(|| scalar_type_error::<Self>(scalar))?;
        Num::try_downcast_scalar(decimal)
            .map(|v| (v, decimal.size()))
            .ok_or_else(|| scalar_type_error::<Self>(scalar))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        let decimal = domain
            .as_decimal()
            .ok_or_else(|| domain_type_error::<Self>(domain))?;
        let size = decimal.decimal_size();
        let domain =
            Num::try_downcast_domain(decimal).ok_or_else(|| domain_type_error::<Self>(domain))?;
        Ok(SimpleDomain {
            min: (domain.min, size),
            max: (domain.max, size),
        })
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        Num::try_downcast_column(col)
            .map(|(col, x)| col.into_iter().map(|v| (v, x)).collect())
            .ok_or_else(|| column_type_error::<Self>(col))
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index).copied()
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        *col.get_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter().copied()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(&rhs)
    }
}

impl<Num: Decimal> SimpleType for CoreScalarDecimal<Num> {
    type Scalar = (Num, DecimalSize);
    type Domain = SimpleDomain<(Num, DecimalSize)>;

    fn downcast_scalar(scalar: &ScalarRef) -> Option<Self::Scalar> {
        let scalar = scalar.as_decimal()?;
        Num::try_downcast_scalar(scalar).map(|v| (v, scalar.size()))
    }

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        Num::try_downcast_column(col).map(|(col, x)| col.into_iter().map(|v| (v, x)).collect())
    }

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        let size = domain.as_decimal()?.decimal_size();
        let domain = Num::try_downcast_domain(domain.as_decimal()?)?;
        Some(SimpleDomain {
            min: (domain.min, size),
            max: (domain.max, size),
        })
    }

    // It's not allowed to call downcast_builder temporarily
    fn downcast_builder(_builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        None
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        let size = builder.as_decimal()?.decimal_size();
        let b = Num::try_downcast_owned_builder(builder);
        b.map(|v| v.into_iter().map(|v| (v, size)).collect())
    }

    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Decimal(Num::upcast_builder(
            builder.into_iter().map(|(v, _)| v).collect(),
            *data_type.as_decimal().unwrap(),
        )))
    }

    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        let size = *data_type.as_decimal().unwrap();
        Num::upcast_scalar(scalar.0, size)
    }

    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        let col = col.into_iter().map(|(v, _)| v).collect();
        let size = *data_type.as_decimal().unwrap();
        Num::upcast_column(col, size)
    }

    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain {
        let domain = SimpleDomain {
            min: domain.min.0,
            max: domain.max.0,
        };
        let size = *data_type.as_decimal().unwrap();
        Num::upcast_domain(domain, size)
    }

    #[inline(always)]
    fn compare(lhs: &Self::Scalar, rhs: &Self::Scalar) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn greater_than(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left > right
    }

    #[inline(always)]
    fn greater_than_equal(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left >= right
    }

    #[inline(always)]
    fn less_than(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left < right
    }

    #[inline(always)]
    fn less_than_equal(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        left <= right
    }
}

impl<Num: Decimal> DecimalType<Num> {
    pub fn full_domain(size: &DecimalSize) -> SimpleDomain<Num> {
        SimpleDomain {
            min: Num::min_for_precision(size.precision),
            max: Num::max_for_precision(size.precision),
        }
    }
}

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    EnumAsInner,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
pub enum DecimalScalar {
    Decimal64(i64, DecimalSize),
    Decimal128(i128, DecimalSize),
    Decimal256(i256, DecimalSize),
}

impl DecimalScalar {
    pub fn to_float32(&self) -> f32 {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(v, size) => v.to_float32(size.scale),
        })
    }

    pub fn to_float64(&self) -> f64 {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(v, size) => v.to_float64(size.scale),
        })
    }

    pub fn is_positive(&self) -> bool {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(v, _) => v.is_positive(),
        })
    }

    pub fn size(&self) -> DecimalSize {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(_, size) => *size,
        })
    }

    pub fn scale(&self) -> u8 {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(_, size) => size.scale,
        })
    }

    pub fn as_decimal<D: Decimal>(&self) -> D {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(value, _) => value.as_decimal(),
        })
    }
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum DecimalColumn {
    Decimal64(Buffer<i64>, DecimalSize),
    Decimal128(Buffer<i128>, DecimalSize),
    Decimal256(Buffer<i256>, DecimalSize),
}

#[derive(Clone, PartialEq, EnumAsInner, Debug)]
pub enum DecimalColumnVec {
    Decimal64(Vec<Buffer<i64>>, DecimalSize),
    Decimal128(Vec<Buffer<i128>>, DecimalSize),
    Decimal256(Vec<Buffer<i256>>, DecimalSize),
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum DecimalColumnBuilder {
    Decimal64(Vec<i64>, DecimalSize),
    Decimal128(Vec<i128>, DecimalSize),
    Decimal256(Vec<i256>, DecimalSize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum DecimalDomain {
    Decimal64(SimpleDomain<i64>, DecimalSize),
    Decimal128(SimpleDomain<i128>, DecimalSize),
    Decimal256(SimpleDomain<i256>, DecimalSize),
}

impl DecimalDomain {
    pub fn decimal_size(&self) -> DecimalSize {
        with_decimal_type!(|DECIMAL| match self {
            DecimalDomain::DECIMAL(_, size) => *size,
        })
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct DecimalSize {
    precision: u8,
    scale: u8,
}

impl Default for DecimalSize {
    fn default() -> Self {
        DecimalSize {
            precision: 15,
            scale: 2,
        }
    }
}

impl DecimalSize {
    pub fn new_unchecked(precision: u8, scale: u8) -> DecimalSize {
        DecimalSize { precision, scale }
    }

    pub fn new(precision: u8, scale: u8) -> Result<DecimalSize> {
        let size = DecimalSize { precision, scale };
        size.validate()?;

        Ok(size)
    }

    pub fn validate(&self) -> Result<()> {
        if self.precision < 1 || self.precision > i256::MAX_PRECISION {
            return Err(ErrorCode::Overflow(format!(
                "Decimal precision must be between 1 and {}",
                i256::MAX_PRECISION
            )));
        }

        if self.scale > self.precision {
            return Err(ErrorCode::Overflow(format!(
                "Decimal scale must be between 0 and precision {}",
                self.precision
            )));
        }

        Ok(())
    }

    pub fn default_128() -> DecimalSize {
        i128::default_decimal_size()
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }

    pub fn leading_digits(&self) -> u8 {
        self.precision - self.scale
    }

    pub fn can_carried_by_64(&self) -> bool {
        self.precision <= i64::MAX_PRECISION
    }

    pub fn can_carried_by_128(&self) -> bool {
        self.precision <= i128::MAX_PRECISION
    }

    pub fn data_kind(&self) -> DecimalDataKind {
        (*self).into()
    }
}

impl From<DecimalSize> for DecimalDataType {
    fn from(size: DecimalSize) -> Self {
        with_decimal_type!(|DECIMAL| match DecimalDataKind::from(size) {
            DecimalDataKind::DECIMAL => DecimalDataType::DECIMAL(size),
        })
    }
}

pub trait Decimal:
    Sized
    + Default
    + Debug
    + std::fmt::Display
    + Copy
    + Clone
    + PartialEq
    + Eq
    + std::ops::AddAssign
    + PartialOrd
    + Ord
    + Sync
    + Send
    + 'static
{
    // the Layout align size of i128 and i256 have changed
    // https://blog.rust-lang.org/2024/03/30/i128-layout-update.html
    // Here we keep this struct in aggregate state which minimize the align of the struct
    type U64Array: Send + Sync + Copy + Default + Debug;
    fn zero() -> Self;
    fn one() -> Self;
    fn minus_one() -> Self;

    // 10**scale
    fn e(n: u8) -> Self;
    fn mem_size() -> usize;

    fn to_u64_array(self) -> Self::U64Array;
    fn from_u64_array(v: Self::U64Array) -> Self;

    fn signum(self) -> Self;

    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn checked_sub(self, rhs: Self) -> Option<Self>;
    fn checked_div(self, rhs: Self) -> Option<Self>;
    fn checked_mul(self, rhs: Self) -> Option<Self>;
    fn checked_rem(self, rhs: Self) -> Option<Self>;

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self>;

    // mul two decimals and return a decimal with rounding option
    fn do_round_mul(self, rhs: Self, shift_scale: u32, overflow: bool) -> Option<Self>;

    fn min_for_precision(precision: u8) -> Self;
    fn max_for_precision(precision: u8) -> Self;

    fn int_part_is_zero(self, scale: u8) -> bool {
        if self >= Self::zero() {
            self <= Self::max_for_precision(scale)
        } else {
            self >= Self::min_for_precision(scale)
        }
    }

    const MAX_PRECISION: u8;
    fn default_decimal_size() -> DecimalSize;

    fn from_float(value: f64) -> Self;

    fn from_i64(value: i64) -> Self;
    fn from_i128(value: impl Into<i128>) -> Option<Self>;
    fn from_i128_uncheck(value: i128) -> Self;
    fn from_i256_uncheck(value: i256) -> Self;
    fn from_bigint(value: BigInt) -> Option<Self>;

    fn de_binary(bytes: &mut &[u8]) -> Self;
    fn to_decimal_string(self, scale: u8) -> String;

    fn to_float32(self, scale: u8) -> f32;
    fn to_float64(self, scale: u8) -> f64;

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U>;

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)>;
    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>>;

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>>;

    fn try_downcast_scalar(scalar: &DecimalScalar) -> Option<Self>;
    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>>;

    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar;
    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column;
    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain;
    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder;
    const DECIMAL_MIN: Self;
    const DECIMAL_MAX: Self;

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn;

    fn to_column(value: Vec<Self>, size: DecimalSize) -> DecimalColumn {
        Self::to_column_from_buffer(value.into(), size)
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar;

    fn as_decimal<D: Decimal>(self) -> D;

    fn with_size(&self, size: DecimalSize) -> Option<Self> {
        let multiplier = Self::e(size.scale());
        let min_for_precision = Self::min_for_precision(size.precision());
        let max_for_precision = Self::max_for_precision(size.precision());
        self.checked_mul(multiplier).and_then(|v| {
            if v > max_for_precision || v < min_for_precision {
                None
            } else {
                Some(v)
            }
        })
    }
}

impl Decimal for i64 {
    type U64Array = [u64; 1];

    fn to_u64_array(self) -> Self::U64Array {
        unsafe { std::mem::transmute(self) }
    }

    fn from_u64_array(v: Self::U64Array) -> Self {
        unsafe { std::mem::transmute(v) }
    }

    fn zero() -> Self {
        0
    }

    fn one() -> Self {
        1
    }

    fn minus_one() -> Self {
        -1
    }

    fn e(n: u8) -> Self {
        const L: usize = i64::MAX_PRECISION as usize + 1;
        const TAB: [i64; L] = {
            const fn gen() -> [i64; L] {
                let mut arr = [0; L];
                let mut i = 0;
                loop {
                    if i == L {
                        break;
                    }
                    arr[i] = 10_i64.pow(i as u32);
                    i += 1;
                }
                arr
            }
            gen()
        };
        TAB.get(n as usize)
            .copied()
            .unwrap_or_else(|| 10_i64.pow(n as u32))
    }

    fn mem_size() -> usize {
        std::mem::size_of::<Self>()
    }

    fn signum(self) -> Self {
        self.signum()
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }

    fn do_round_mul(self, rhs: Self, shift_scale: u32, overflow: bool) -> Option<Self> {
        if shift_scale == 0 {
            return Some(self);
        }

        if !overflow {
            let div = i64::e(shift_scale as u8);
            let res = if self.is_negative() == rhs.is_negative() {
                (self * rhs + div / 2) / div
            } else {
                (self * rhs - div / 2) / div
            };
            return Some(res);
        }

        let div = i128::e(shift_scale as u8);
        let res = if self.is_negative() == rhs.is_negative() {
            (self as i128 * rhs as i128 + div / 2) / div
        } else {
            (self as i128 * rhs as i128 - div / 2) / div
        };

        if !(i64::DECIMAL_MIN as i128..=i64::DECIMAL_MAX as i128).contains(&res) {
            None
        } else {
            Some(res as i64)
        }
    }

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self> {
        let mul = i128::e(mul_scale as u8);
        let rhs = rhs as i128;
        let res = if self.is_negative() == rhs.is_negative() {
            ((self as i128) * mul + rhs / 2) / rhs
        } else {
            ((self as i128) * mul - rhs / 2) / rhs
        };
        Some(res as i64)
    }

    fn min_for_precision(precision: u8) -> Self {
        -(Self::e(precision) - 1)
    }

    fn max_for_precision(precision: u8) -> Self {
        Self::e(precision) - 1
    }

    const MAX_PRECISION: u8 = 18;
    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: Self::MAX_PRECISION,
            scale: 0,
        }
    }

    fn from_float(value: f64) -> Self {
        value as i64
    }

    #[inline]
    fn from_i64(value: i64) -> Self {
        value
    }

    #[inline]
    fn from_i128(value: impl Into<i128>) -> Option<Self> {
        i64::try_from(value.into()).ok()
    }

    #[inline]
    fn from_i128_uncheck(value: i128) -> Self {
        value as i64
    }

    #[inline]
    fn from_i256_uncheck(value: i256) -> Self {
        value.as_i64()
    }

    fn from_bigint(value: BigInt) -> Option<Self> {
        value.to_i64()
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i64::from_le_bytes(bs)
    }

    fn to_decimal_string(self, scale: u8) -> String {
        display_decimal_128(self as i128, scale).to_string()
    }

    fn to_float32(self, scale: u8) -> f32 {
        let div = 10_f32.powi(scale as i32);
        self as f32 / div
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self as f64 / div
    }

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U> {
        let div = 10i64.checked_pow(scale as u32)?;
        let mut val = self / div;
        if rounding_mode && scale > 0 {
            if let Some(r) = self.checked_rem(div) {
                if let Some(m) = r.checked_div(i64::e((scale as u32 - 1) as u8)) {
                    if m >= 5i64 {
                        val = val.checked_add(1i64)?;
                    } else if m <= -5i64 {
                        val = val.checked_sub(1i64)?;
                    }
                }
            }
        }
        num_traits::cast(val)
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar {
        DecimalScalar::Decimal64(self, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal64(c, size) => Some((c.clone(), *size)),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal64(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal64(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_scalar(scalar: &DecimalScalar) -> Option<Self> {
        match scalar {
            DecimalScalar::Decimal64(val, _) => Some(*val),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>> {
        match domain {
            DecimalDomain::Decimal64(val, _) => Some(*val),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal64(scalar, size))
    }

    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column {
        Column::Decimal(DecimalColumn::Decimal64(col, size))
    }

    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain {
        Domain::Decimal(DecimalDomain::Decimal64(domain, size))
    }

    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder {
        DecimalColumnBuilder::Decimal64(builder, size)
    }

    const DECIMAL_MIN: i64 = -999999999999999999;
    const DECIMAL_MAX: i64 = 999999999999999999;

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal64(value, size)
    }

    #[inline]
    fn as_decimal<D: Decimal>(self) -> D {
        D::from_i64(self)
    }
}

impl Decimal for i128 {
    type U64Array = [u64; 2];

    fn to_u64_array(self) -> Self::U64Array {
        unsafe { std::mem::transmute(self) }
    }

    fn from_u64_array(v: Self::U64Array) -> Self {
        unsafe { std::mem::transmute(v) }
    }

    fn zero() -> Self {
        0_i128
    }

    fn one() -> Self {
        1_i128
    }

    fn minus_one() -> Self {
        -1_i128
    }

    fn e(n: u8) -> Self {
        const L: usize = i128::MAX_PRECISION as usize + 1;
        const TAB: [i128; L] = {
            const fn gen() -> [i128; L] {
                let mut arr = [0; L];
                let mut i = 0;
                loop {
                    if i == L {
                        break;
                    }
                    arr[i] = 10_i128.pow(i as u32);
                    i += 1;
                }
                arr
            }
            gen()
        };
        TAB.get(n as usize)
            .copied()
            .unwrap_or_else(|| 10_i128.pow(n as u32))
    }

    fn mem_size() -> usize {
        16
    }

    fn signum(self) -> Self {
        self.signum()
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }

    fn do_round_mul(self, rhs: Self, shift_scale: u32, overflow: bool) -> Option<Self> {
        if shift_scale == 0 {
            return Some(self);
        }

        if !overflow {
            let div = i128::e(shift_scale as u8);
            let res = if self.is_negative() == rhs.is_negative() {
                (self * rhs + div / 2) / div
            } else {
                (self * rhs - div / 2) / div
            };

            return Some(res);
        }

        let div = i256::e(shift_scale as u8);
        let res = if self.is_negative() == rhs.is_negative() {
            (i256::from(self) * i256::from(rhs) + div / i256::from(2)) / div
        } else {
            (i256::from(self) * i256::from(rhs) - div / i256::from(2)) / div
        };

        if !(i256::from(i128::MIN)..=i256::from(i128::MAX)).contains(&res) {
            None
        } else {
            Some(res.as_i128())
        }
    }

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self> {
        let mul = i256::e(mul_scale as u8);
        let rhs = i256::from(rhs);
        let res = if self.is_negative() == rhs.is_negative() {
            (i256::from(self) * mul + rhs / i256::from(2)) / rhs
        } else {
            (i256::from(self) * mul - rhs / i256::from(2)) / rhs
        };
        Some(*res.low())
    }

    fn min_for_precision(to_precision: u8) -> Self {
        /// `MIN_DECIMAL_FOR_EACH_PRECISION[p]` holds the minimum `i128` value that can
        /// be stored in a [arrow_schema::DataType::Decimal128] value of precision `p`
        const MIN_DECIMAL_FOR_EACH_PRECISION: [i128; 38] = {
            const fn gen() -> [i128; 38] {
                let mut arr = [0; 38];
                let mut i = 0;
                loop {
                    if i == 38 {
                        break;
                    }
                    arr[i] = -(10_i128.pow(1 + i as u32) - 1);
                    i += 1;
                }
                arr
            }
            gen()
        };

        MIN_DECIMAL_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    fn max_for_precision(to_precision: u8) -> Self {
        /// `MAX_DECIMAL_FOR_EACH_PRECISION[p]` holds the maximum `i128` value that can
        /// be stored in [arrow_schema::DataType::Decimal128] value of precision `p`
        const MAX_DECIMAL_FOR_EACH_PRECISION: [i128; 38] = {
            const fn gen() -> [i128; 38] {
                let mut arr = [0; 38];
                let mut i = 0;
                loop {
                    if i == 38 {
                        break;
                    }
                    arr[i] = 10_i128.pow(1 + i as u32) - 1;
                    i += 1;
                }
                arr
            }
            gen()
        };
        MAX_DECIMAL_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    const MAX_PRECISION: u8 = 38;
    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: Self::MAX_PRECISION,
            scale: 0,
        }
    }

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal128(value, size)
    }

    #[inline]
    fn as_decimal<D: Decimal>(self) -> D {
        D::from_i128_uncheck(self)
    }

    fn from_float(value: f64) -> Self {
        // still needs to be optimized.
        // An implementation similar to float64_as_i256 obtained from the ethnum library
        const M: u64 = (f64::MANTISSA_DIGITS - 1) as u64;
        const MAN_MASK: u64 = !(!0 << M);
        const MAN_ONE: u64 = 1 << M;
        const EXP_MASK: u64 = !0 >> f64::MANTISSA_DIGITS;
        const EXP_OFFSET: u64 = EXP_MASK / 2;
        const ABS_MASK: u64 = !0 >> 1;
        const SIG_MASK: u64 = !ABS_MASK;

        let abs = f64::from_bits(value.to_bits() & ABS_MASK);
        let sign = -(((value.to_bits() & SIG_MASK) >> (u64::BITS - 2)) as i128).wrapping_sub(1); // if self >= 0. { 1 } else { -1 }
        if abs >= 1.0 {
            let bits = abs.to_bits();
            let exponent = ((bits >> M) & EXP_MASK) - EXP_OFFSET;
            let mantissa = (bits & MAN_MASK) | MAN_ONE;
            if exponent <= M {
                (<i128 as From<u64>>::from(mantissa >> (M - exponent))) * sign
            } else if exponent < 127 {
                (<i128 as From<u64>>::from(mantissa) << (exponent - M)) * sign
            } else if sign > 0 {
                i128::MAX
            } else {
                i128::MIN
            }
        } else {
            Self::zero()
        }
    }

    #[inline]
    fn from_i64(value: i64) -> Self {
        value as _
    }

    #[inline]
    fn from_i128(value: impl Into<i128>) -> Option<Self> {
        Some(value.into())
    }

    #[inline]
    fn from_i128_uncheck(value: i128) -> Self {
        value
    }

    #[inline]
    fn from_i256_uncheck(value: i256) -> Self {
        value.as_i128()
    }

    fn from_bigint(value: BigInt) -> Option<Self> {
        value.to_i128()
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i128::from_le_bytes(bs)
    }

    fn to_decimal_string(self, scale: u8) -> String {
        display_decimal_128(self, scale).to_string()
    }

    fn to_float32(self, scale: u8) -> f32 {
        let div = 10_f32.powi(scale as i32);
        self as f32 / div
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self as f64 / div
    }

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U> {
        let div = 10i128.checked_pow(scale as u32)?;
        let mut val = self / div;
        if rounding_mode && scale > 0 {
            // Checking whether numbers need to be added or subtracted to calculate rounding
            if let Some(r) = self.checked_rem(div) {
                if let Some(m) = r.checked_div(i128::e(scale - 1)) {
                    if m >= 5i128 {
                        val = val.checked_add(1i128)?;
                    } else if m <= -5i128 {
                        val = val.checked_sub(1i128)?;
                    }
                }
            }
        }
        num_traits::cast(val)
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar {
        DecimalScalar::Decimal128(self, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal128(c, size) => Some((c.clone(), *size)),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_scalar<'a>(scalar: &DecimalScalar) -> Option<Self> {
        match scalar {
            DecimalScalar::Decimal128(val, _) => Some(*val),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>> {
        match domain {
            DecimalDomain::Decimal128(val, _) => Some(*val),
            _ => None,
        }
    }

    // will mock DecimalSize need modify when use it
    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal128(scalar, size))
    }

    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column {
        Column::Decimal(DecimalColumn::Decimal128(col, size))
    }

    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain {
        Domain::Decimal(DecimalDomain::Decimal128(domain, size))
    }

    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder {
        DecimalColumnBuilder::Decimal128(builder, size)
    }

    const DECIMAL_MIN: i128 = -99999999999999999999999999999999999999i128;

    const DECIMAL_MAX: i128 = 99999999999999999999999999999999999999i128;
}

impl Decimal for i256 {
    type U64Array = [u64; 4];

    fn to_u64_array(self) -> Self::U64Array {
        unsafe { std::mem::transmute(self) }
    }

    fn from_u64_array(v: Self::U64Array) -> Self {
        unsafe { std::mem::transmute(v) }
    }

    fn zero() -> Self {
        i256::ZERO
    }

    fn one() -> Self {
        i256::ONE
    }

    fn minus_one() -> Self {
        i256::MINUS_ONE
    }

    fn e(n: u8) -> Self {
        match n {
            0 => i256::ONE,
            1..=i256::MAX_PRECISION => {
                MAX_DECIMAL256_BYTES_FOR_EACH_PRECISION[n as usize - 1] + i256::ONE
            }
            _ => i256::from(10).pow(n as u32),
        }
    }

    fn mem_size() -> usize {
        32
    }

    fn signum(self) -> Self {
        Self(self.0.signum())
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.0.checked_add(rhs.0).map(Self)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.0.checked_sub(rhs.0).map(Self)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.0.checked_div(rhs.0).map(Self)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.0.checked_mul(rhs.0).map(Self)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.0.checked_rem(rhs.0).map(Self)
    }

    fn do_round_mul(self, rhs: Self, shift_scale: u32, overflow: bool) -> Option<Self> {
        if shift_scale == 0 {
            return Some(self);
        }

        let div = i256::e(shift_scale as u8);
        if !overflow {
            let ret = if self.is_negative() == rhs.is_negative() {
                (self * rhs + div / i256::from(2)) / div
            } else {
                (self * rhs - div / i256::from(2)) / div
            };
            return Some(ret);
        }

        let ret: Option<i256> = if self.is_negative() == rhs.is_negative() {
            self.checked_mul(rhs)
                .map(|x| (x + div / i256::from(2)) / div)
        } else {
            self.checked_mul(rhs)
                .map(|x| (x - div / i256::from(2)) / div)
        };

        ret.or_else(|| {
            let a = BigInt::from_le_bytes(&self.to_le_bytes());
            let b = BigInt::from_le_bytes(&rhs.to_le_bytes());
            let div = BigInt::from(10).pow(shift_scale);
            if self.is_negative() == rhs.is_negative() {
                Self::from_bigint((a * b + div.clone() / 2) / div)
            } else {
                Self::from_bigint((a * b - div.clone() / 2) / div)
            }
        })
    }

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self> {
        let fallback = || {
            let a = BigInt::from_le_bytes(&self.to_le_bytes());
            let b = BigInt::from_le_bytes(&rhs.to_le_bytes());
            let mul = BigInt::from(10).pow(mul_scale);
            if self.is_negative() == rhs.is_negative() {
                Self::from_bigint((a * mul + b.clone() / 2) / b)
            } else {
                Self::from_bigint((a * mul - b.clone() / 2) / b)
            }
        };

        if mul_scale >= i256::MAX_PRECISION as _ {
            return fallback();
        }

        let mul = i256::e(mul_scale as u8);
        let ret: Option<i256> = if self.is_negative() == rhs.is_negative() {
            self.checked_mul(mul)
                .map(|x| (x + rhs / i256::from(2)) / rhs)
        } else {
            self.checked_mul(mul)
                .map(|x| (x - rhs / i256::from(2)) / rhs)
        };

        ret.or_else(fallback)
    }

    fn min_for_precision(to_precision: u8) -> Self {
        MIN_DECIMAL256_BYTES_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    fn max_for_precision(to_precision: u8) -> Self {
        MAX_DECIMAL256_BYTES_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    const MAX_PRECISION: u8 = 76;
    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: i256::MAX_PRECISION,
            scale: 0,
        }
    }

    fn from_float(value: f64) -> Self {
        i256(value.as_i256())
    }

    #[inline]
    fn from_i64(value: i64) -> Self {
        i256::from(value)
    }

    #[inline]
    fn from_i128(value: impl Into<i128>) -> Option<Self> {
        Some(i256::from(value.into()))
    }

    #[inline]
    fn from_i128_uncheck(value: i128) -> Self {
        i256::from(value)
    }

    #[inline]
    fn from_i256_uncheck(value: i256) -> Self {
        value
    }

    fn from_bigint(value: BigInt) -> Option<Self> {
        let mut ret: u256 = u256::ZERO;
        let mut bits = 0;

        for i in value.iter_u64_digits() {
            if bits >= 256 {
                return None;
            }
            ret |= u256::from(i) << bits;
            bits += 64;
        }

        match value.sign() {
            num_bigint::Sign::Plus => i256::try_from(ret).ok(),
            num_bigint::Sign::NoSign => Some(i256::ZERO),
            num_bigint::Sign::Minus => {
                let m: u256 = u256::ONE << 255;
                match ret.cmp(&m) {
                    Ordering::Less => Some(-i256::try_from(ret).unwrap()),
                    Ordering::Equal => Some(i256::DECIMAL_MIN),
                    Ordering::Greater => None,
                }
            }
        }
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i256::from_le_bytes(bs)
    }

    fn to_decimal_string(self, scale: u8) -> String {
        display_decimal_256(self.0, scale).to_string()
    }

    fn to_float32(self, scale: u8) -> f32 {
        let div = 10_f32.powi(scale as i32);
        self.as_f32() / div
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self.as_f64() / div
    }

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U> {
        if !(i256::from(i128::MIN)..=i256::from(i128::MAX)).contains(&self) {
            None
        } else {
            let val = self.as_i128();
            val.to_int(scale, rounding_mode)
        }
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar {
        DecimalScalar::Decimal256(self, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal256(c, size) => Some((c.clone(), *size)),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_scalar<'a>(scalar: &DecimalScalar) -> Option<Self> {
        match scalar {
            DecimalScalar::Decimal256(val, _) => Some(*val),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>> {
        match domain {
            DecimalDomain::Decimal256(val, _) => Some(*val),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal256(scalar, size))
    }

    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column {
        Column::Decimal(DecimalColumn::Decimal256(col, size))
    }

    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain {
        Domain::Decimal(DecimalDomain::Decimal256(domain, size))
    }

    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder {
        DecimalColumnBuilder::Decimal256(builder, size)
    }

    const DECIMAL_MIN: i256 = i256(ethnum::int!(
        "-9999999999999999999999999999999999999999999999999999999999999999999999999999"
    ));
    const DECIMAL_MAX: i256 = i256(ethnum::int!(
        "9999999999999999999999999999999999999999999999999999999999999999999999999999"
    ));
    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal256(value, size)
    }

    #[inline]
    fn as_decimal<D: Decimal>(self) -> D {
        D::from_i256_uncheck(self)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    EnumAsInner,
)]
pub enum DecimalDataType {
    Decimal64(DecimalSize),
    Decimal128(DecimalSize),
    Decimal256(DecimalSize),
}

impl DecimalDataType {
    pub fn from_value(value: &Value<AnyType>) -> Option<(DecimalDataType, bool)> {
        match value {
            Value::Scalar(Scalar::Decimal(scalar)) => with_decimal_type!(|T| match scalar {
                DecimalScalar::T(_, size) => Some((DecimalDataType::T(*size), false)),
            }),
            Value::Column(Column::Decimal(column)) => with_decimal_type!(|T| match column {
                DecimalColumn::T(_, size) => Some((DecimalDataType::T(*size), false)),
            }),
            Value::Column(Column::Nullable(box column)) => {
                with_decimal_type!(|T| match &column.column {
                    Column::Decimal(DecimalColumn::T(_, size)) =>
                        Some((DecimalDataType::T(*size), true)),
                    _ => None,
                })
            }
            _ => None,
        }
    }

    pub fn data_kind(&self) -> DecimalDataKind {
        with_decimal_type!(|DECIMAL| match self {
            DecimalDataType::DECIMAL(_) => DecimalDataKind::DECIMAL,
        })
    }

    pub fn default_scalar(&self) -> DecimalScalar {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => DecimalScalar::DECIMAL_TYPE(0.into(), *size),
        })
    }

    pub fn size(&self) -> DecimalSize {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => *size,
        })
    }

    pub fn scale(&self) -> u8 {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.scale(),
        })
    }

    pub fn precision(&self) -> u8 {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.precision(),
        })
    }

    pub fn leading_digits(&self) -> u8 {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.precision() - size.scale(),
        })
    }

    pub fn max_precision(&self) -> u8 {
        match self {
            DecimalDataType::Decimal64(_) => i64::MAX_PRECISION,
            DecimalDataType::Decimal128(_) => i128::MAX_PRECISION,
            DecimalDataType::Decimal256(_) => i256::MAX_PRECISION,
        }
    }

    pub fn max_result_precision(&self, other: &Self) -> u8 {
        self.max_precision().max(other.max_precision())
    }

    // For div ops, a,b and return must belong to same width types
    pub fn div_common_type(a: &Self, b: &Self, return_size: DecimalSize) -> Result<(Self, Self)> {
        let return_kind = DecimalDataKind::from(return_size);

        let a = if a.data_kind() == return_kind {
            *a
        } else {
            DecimalSize::new(return_size.precision, a.scale())?.into()
        };

        let b = if b.data_kind() == return_kind {
            *b
        } else {
            DecimalSize::new(return_size.precision, b.scale())?.into()
        };

        Ok((a, b))
    }

    pub fn is_strict(&self) -> bool {
        DecimalDataKind::from(self.size()) == self.data_kind()
    }
}

impl PartialOrd for DecimalDataType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DecimalDataType {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_precision = self.precision();
        let other_precision = other.precision();
        let self_scale = self.scale();
        let other_scale = other.scale();
        if self_precision == other_precision {
            self_scale.cmp(&other_scale)
        } else {
            self_precision.cmp(&other_precision)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalDataKind {
    Decimal64,
    Decimal128,
    Decimal256,
}

impl From<DecimalSize> for DecimalDataKind {
    fn from(size: DecimalSize) -> Self {
        if size.can_carried_by_64() {
            return DecimalDataKind::Decimal64;
        }
        if size.can_carried_by_128() {
            return DecimalDataKind::Decimal128;
        }
        DecimalDataKind::Decimal256
    }
}

impl DecimalDataKind {
    pub fn with_size(&self, size: DecimalSize) -> DecimalDataType {
        match self {
            DecimalDataKind::Decimal64 => DecimalDataType::Decimal64(size),
            DecimalDataKind::Decimal128 => DecimalDataType::Decimal128(size),
            DecimalDataKind::Decimal256 => DecimalDataType::Decimal256(size),
        }
    }
}

impl DecimalScalar {
    pub fn domain(&self) -> DecimalDomain {
        with_decimal_type!(|DECIMAL| match self {
            DecimalScalar::DECIMAL(num, size) => DecimalDomain::DECIMAL(
                SimpleDomain {
                    min: *num,
                    max: *num,
                },
                *size,
            ),
        })
    }
}

impl DecimalColumn {
    pub fn len(&self) -> usize {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, _) => col.len(),
        })
    }

    pub fn index(&self, index: usize) -> Option<DecimalScalar> {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                Some(DecimalScalar::DECIMAL_TYPE(col.get(index).cloned()?, *size)),
        })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> DecimalScalar {
        debug_assert!(index < self.len());
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                DecimalScalar::DECIMAL_TYPE(*col.get_unchecked(index), *size),
        })
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );

        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) => {
                DecimalColumn::DECIMAL_TYPE(
                    col.clone().sliced(range.start, range.end - range.start),
                    *size,
                )
            }
        })
    }

    pub fn domain(&self) -> DecimalDomain {
        assert!(self.len() > 0);
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                DecimalDomain::DECIMAL_TYPE(
                    SimpleDomain {
                        min: *min,
                        max: *max,
                    },
                    *size,
                )
            }
        })
    }

    pub fn arrow_data(&self, arrow_type: arrow_schema::DataType) -> ArrayData {
        #[cfg(debug_assertions)]
        {
            match (&arrow_type, self) {
                (arrow_schema::DataType::Decimal128(p, s), DecimalColumn::Decimal64(_, size))
                | (arrow_schema::DataType::Decimal128(p, s), DecimalColumn::Decimal128(_, size))
                | (arrow_schema::DataType::Decimal256(p, s), DecimalColumn::Decimal256(_, size)) => {
                    assert_eq!(size.precision, *p);
                    assert_eq!(size.scale as i16, *s as i16);
                }
                _ => unreachable!(),
            }
        }

        let buffer = match self {
            DecimalColumn::Decimal64(col, _) => {
                let builder = Decimal64As128Type::iter_column(col).collect::<Vec<_>>();
                Decimal128Type::build_column(builder).into()
            }
            DecimalColumn::Decimal128(col, _) => col.clone().into(),
            DecimalColumn::Decimal256(col, _) => {
                let col = unsafe {
                    std::mem::transmute::<_, Buffer<databend_common_column::types::i256>>(
                        col.clone(),
                    )
                };
                col.into()
            }
        };

        let builder = ArrayDataBuilder::new(arrow_type)
            .len(self.len())
            .buffers(vec![buffer]);
        unsafe { builder.build_unchecked() }
    }

    pub fn try_from_arrow_data(array: ArrayData) -> Result<Self> {
        let buffer = array.buffers()[0].clone();
        match array.data_type() {
            arrow_schema::DataType::Decimal128(p, s) => {
                let decimal_size = DecimalSize {
                    precision: *p,
                    scale: *s as u8,
                };
                Ok(Self::Decimal128(buffer.into(), decimal_size).strict_decimal())
            }
            arrow_schema::DataType::Decimal256(p, s) => {
                let decimal_size = DecimalSize {
                    precision: *p,
                    scale: *s as u8,
                };

                let buffer: Buffer<databend_common_column::types::i256> = buffer.into();
                let buffer = unsafe { std::mem::transmute::<_, Buffer<i256>>(buffer) };
                Ok(Self::Decimal256(buffer, decimal_size))
            }
            data_type => Err(ErrorCode::Unimplemented(format!(
                "Unsupported data type: {:?} into decimal column",
                data_type
            ))),
        }
    }

    pub fn size(&self) -> DecimalSize {
        match self {
            DecimalColumn::Decimal64(_, size)
            | DecimalColumn::Decimal128(_, size)
            | DecimalColumn::Decimal256(_, size) => *size,
        }
    }

    pub fn strict_decimal(self) -> Self {
        if self.is_strict_decimal() {
            return self;
        }

        with_decimal_type!(|FROM| match self {
            DecimalColumn::FROM(buffer, size) => {
                with_decimal_type!(|TO| match size.data_kind() {
                    DecimalDataKind::TO => {
                        let builder = DecimalView::iter_column(&buffer).collect::<Vec<_>>();
                        DecimalColumn::TO(DecimalType::build_column(builder), size)
                    }
                })
            }
        })
    }

    pub fn is_strict_decimal(&self) -> bool {
        self.data_kind() == self.size().data_kind()
    }

    pub fn data_kind(&self) -> DecimalDataKind {
        match self {
            DecimalColumn::Decimal64(_, _) => DecimalDataKind::Decimal64,
            DecimalColumn::Decimal128(_, _) => DecimalDataKind::Decimal128,
            DecimalColumn::Decimal256(_, _) => DecimalDataKind::Decimal256,
        }
    }
}

impl DecimalColumnBuilder {
    pub fn from_column(col: DecimalColumn) -> Self {
        with_decimal_type!(|DECIMAL_TYPE| match col {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(buffer_into_mut(col), size),
        })
    }

    pub fn repeat(scalar: DecimalScalar, n: usize) -> DecimalColumnBuilder {
        with_decimal_type!(|DECIMAL_TYPE| match scalar {
            DecimalScalar::DECIMAL_TYPE(num, size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(vec![num; n], size),
        })
    }

    pub fn repeat_default(ty: &DecimalDataType, n: usize) -> Self {
        with_decimal_type!(|DECIMAL_TYPE| match ty {
            DecimalDataType::DECIMAL_TYPE(size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(vec![0.into(); n], *size),
        })
    }

    pub fn len(&self) -> usize {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(col, _) => col.len(),
        })
    }

    pub fn with_capacity(ty: &DecimalDataType, capacity: usize) -> Self {
        with_decimal_type!(|DECIMAL_TYPE| match ty {
            DecimalDataType::DECIMAL_TYPE(size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(Vec::with_capacity(capacity), *size),
        })
    }

    pub fn push(&mut self, item: DecimalScalar) {
        self.push_repeat(item, 1)
    }

    pub fn push_repeat(&mut self, item: DecimalScalar, n: usize) {
        with_decimal_mapped_type!(|DECIMAL| match self {
            DecimalColumnBuilder::DECIMAL(builder, builder_size) => {
                let value = item.as_decimal::<DECIMAL>();
                debug_assert_eq!(*builder_size, item.size());
                if n == 1 {
                    builder.push(value)
                } else {
                    builder.resize(builder.len() + n, value)
                }
            }
        })
    }

    pub fn push_default(&mut self) {
        with_decimal_type!(|DECIMAL| match self {
            DecimalColumnBuilder::DECIMAL(builder, _) => builder.push(0.into()),
        })
    }

    pub fn append_column(&mut self, column: &DecimalColumn) {
        with_decimal_type!(|DECIMAL_TYPE| match (self, column) {
            (
                DecimalColumnBuilder::DECIMAL_TYPE(builder, builder_size),
                DecimalColumn::DECIMAL_TYPE(column, column_size),
            ) => {
                debug_assert_eq!(builder_size, column_size);
                builder.extend_from_slice(column);
            }
            (_b, _c) => unreachable!(
                // "unable append column(data type: Decimal256) into builder(data type: Decimal128)"
            ),
        })
    }

    pub fn build(self) -> DecimalColumn {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) =>
                DecimalColumn::DECIMAL_TYPE(builder.into(), size),
        })
    }

    pub fn build_scalar(self) -> DecimalScalar {
        assert_eq!(self.len(), 1);

        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) =>
                DecimalScalar::DECIMAL_TYPE(builder[0], size),
        })
    }

    pub fn pop(&mut self) -> Option<DecimalScalar> {
        with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) => {
                builder
                    .pop()
                    .map(|num| DecimalScalar::DECIMAL_TYPE(num, *size))
            }
        })
    }

    pub fn decimal_size(&self) -> DecimalSize {
        with_decimal_type!(|DECIMAL| match self {
            DecimalColumnBuilder::DECIMAL(_, size) => *size,
        })
    }
}

impl PartialOrd for DecimalScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
            (
                DecimalScalar::DECIMAL_TYPE(lhs, lhs_size),
                DecimalScalar::DECIMAL_TYPE(rhs, rhs_size),
            ) => {
                if lhs_size == rhs_size {
                    lhs.partial_cmp(rhs)
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

impl PartialOrd for DecimalColumn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
            (
                DecimalColumn::DECIMAL_TYPE(lhs, lhs_size),
                DecimalColumn::DECIMAL_TYPE(rhs, rhs_size),
            ) => {
                if lhs_size == rhs_size {
                    lhs.iter().partial_cmp(rhs.iter())
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

#[macro_export]
macro_rules! with_decimal_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Decimal64, Decimal128, Decimal256],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_decimal_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Decimal64 => i64, Decimal128 => i128, Decimal256 => i256
            ],
            $($tail)*
        }
    }
}
// MAX decimal256 value of little-endian format for each precision.
// Each element is the max value of signed 256-bit integer for the specified precision which
// is encoded to the 32-byte width format of little-endian.
const MAX_DECIMAL256_BYTES_FOR_EACH_PRECISION: [i256; 76] = [
    i256::from_le_bytes([
        9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        231, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        15, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        159, 134, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,
    ]),
    i256::from_le_bytes([
        63, 66, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,
    ]),
    i256::from_le_bytes([
        127, 150, 152, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 224, 245, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 201, 154, 59, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 227, 11, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 231, 118, 72, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 15, 165, 212, 232, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 159, 114, 78, 24, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 63, 122, 16, 243, 90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 127, 198, 164, 126, 141, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 192, 111, 242, 134, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 137, 93, 120, 69, 99, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 99, 167, 179, 182, 224, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 231, 137, 4, 35, 199, 138, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 15, 99, 45, 94, 199, 107, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 159, 222, 197, 173, 201, 53, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 63, 178, 186, 201, 224, 25, 30, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 127, 246, 74, 225, 199, 2, 45, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 160, 237, 204, 206, 27, 194, 211, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 73, 72, 1, 20, 22, 149, 69, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 227, 210, 12, 200, 220, 210, 183, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 231, 60, 128, 208, 159, 60, 46, 59, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 15, 97, 2, 37, 62, 94, 206, 79, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 159, 202, 23, 114, 109, 174, 15, 30, 67, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 63, 234, 237, 116, 70, 208, 156, 44, 159, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 127, 38, 75, 145, 192, 34, 32, 190, 55, 126, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 128, 239, 172, 133, 91, 65, 109, 45, 238, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 9, 91, 193, 56, 147, 141, 68, 198, 77, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 99, 142, 141, 55, 192, 135, 173, 190, 9, 237, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 231, 143, 135, 43, 130, 77, 199, 114, 97, 66, 19, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 15, 159, 75, 179, 21, 7, 201, 123, 206, 151, 192, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 159, 54, 244, 0, 217, 70, 218, 213, 16, 238, 133, 7, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 127, 86, 101, 95, 196, 172, 67, 137, 147, 254, 80, 240, 2, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 96, 245, 185, 171, 191, 164, 92, 195, 241, 41, 99, 29, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 201, 149, 67, 181, 124, 111, 158, 161, 113, 163, 223, 37, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 227, 217, 163, 20, 223, 90, 48, 80, 112, 98, 188, 122, 11, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 231, 130, 102, 206, 182, 140, 227, 33, 99, 216, 91, 203, 114, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 15, 29, 1, 16, 36, 127, 227, 82, 223, 115, 150, 241, 123, 4, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 159, 34, 11, 160, 104, 247, 226, 60, 185, 134, 224, 111, 215, 44,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 63, 90, 111, 64, 22, 170, 221, 96, 60, 67, 197, 94, 106, 192, 1,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 127, 134, 89, 132, 222, 164, 168, 200, 91, 160, 180, 179, 39, 132,
        17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 64, 127, 43, 177, 112, 150, 214, 149, 67, 14, 5, 141, 41,
        175, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 137, 248, 178, 235, 102, 224, 97, 218, 163, 142, 50, 130,
        159, 215, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 99, 181, 253, 52, 5, 196, 210, 135, 102, 146, 249, 21, 59,
        108, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 231, 21, 233, 17, 52, 168, 59, 78, 1, 184, 191, 219, 78, 58,
        172, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 15, 219, 26, 179, 8, 146, 84, 14, 13, 48, 125, 149, 20, 71,
        186, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 159, 142, 12, 255, 86, 180, 77, 143, 130, 224, 227, 214, 205,
        198, 70, 11, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 63, 146, 125, 246, 101, 11, 9, 153, 25, 197, 230, 100, 10,
        196, 195, 112, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 127, 182, 231, 160, 251, 113, 90, 250, 255, 178, 3, 241, 103,
        168, 165, 103, 104, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 32, 13, 73, 212, 115, 136, 199, 255, 253, 36, 106, 15,
        148, 120, 12, 20, 4, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 73, 131, 218, 74, 134, 84, 203, 253, 235, 113, 37, 154,
        200, 181, 124, 200, 40, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 227, 32, 137, 236, 62, 77, 241, 233, 55, 115, 118, 5,
        214, 25, 223, 212, 151, 1, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 231, 72, 91, 61, 117, 4, 109, 35, 47, 128, 160, 54, 92,
        2, 183, 80, 238, 15, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 15, 217, 144, 101, 148, 44, 66, 98, 215, 1, 69, 34, 154,
        23, 38, 39, 79, 159, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 159, 122, 168, 247, 203, 189, 149, 214, 105, 18, 178,
        86, 5, 236, 124, 135, 23, 57, 6, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 63, 202, 148, 172, 247, 105, 217, 97, 34, 184, 244, 98,
        53, 56, 225, 74, 235, 58, 62, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 127, 230, 207, 189, 172, 35, 126, 210, 87, 49, 143, 221,
        21, 50, 204, 236, 48, 77, 110, 2, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 0, 31, 106, 191, 100, 237, 56, 110, 237, 151, 167,
        218, 244, 249, 63, 233, 3, 79, 24, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 9, 54, 37, 122, 239, 69, 57, 78, 70, 239, 139, 138,
        144, 195, 127, 28, 39, 22, 243, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 99, 28, 116, 197, 90, 187, 60, 14, 191, 88, 119,
        105, 165, 163, 253, 28, 135, 221, 126, 9, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 231, 27, 137, 182, 139, 81, 95, 142, 118, 119, 169,
        30, 118, 100, 232, 33, 71, 167, 244, 94, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 15, 23, 91, 33, 117, 47, 185, 143, 161, 170, 158,
        50, 157, 236, 19, 83, 199, 136, 142, 181, 3, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 159, 230, 142, 77, 147, 218, 59, 157, 79, 170, 50,
        250, 35, 62, 199, 62, 201, 87, 145, 23, 37, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 63, 2, 149, 7, 193, 137, 86, 36, 28, 167, 250, 197,
        103, 109, 200, 115, 220, 109, 173, 235, 114, 1, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 127, 22, 210, 75, 138, 97, 97, 107, 25, 135, 202,
        187, 13, 70, 212, 133, 156, 74, 198, 52, 125, 14, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 224, 52, 246, 102, 207, 205, 49, 254, 70, 233,
        85, 137, 188, 74, 58, 29, 234, 190, 15, 228, 144, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 201, 16, 158, 5, 26, 10, 242, 237, 197, 28,
        91, 93, 93, 235, 70, 36, 37, 117, 157, 232, 168, 5, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 227, 167, 44, 56, 4, 101, 116, 75, 187, 31,
        143, 165, 165, 49, 197, 106, 115, 147, 38, 22, 153, 56, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 231, 142, 190, 49, 42, 242, 139, 242, 80, 61,
        151, 119, 120, 240, 179, 43, 130, 194, 129, 221, 250, 53, 2,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 15, 149, 113, 241, 165, 117, 119, 121, 41,
        101, 232, 171, 180, 100, 7, 181, 21, 153, 17, 167, 204, 27, 22,
    ]),
];

// MIN decimal256 value of little-endian format for each precision.
// Each element is the min value of signed 256-bit integer for the specified precision which
// is encoded to the 76-byte width format of little-endian.
const MIN_DECIMAL256_BYTES_FOR_EACH_PRECISION: [i256; 76] = [
    i256::from_le_bytes([
        247, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        157, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        25, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        241, 216, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        97, 121, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        193, 189, 240, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        129, 105, 103, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 31, 10, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 54, 101, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 28, 244, 171, 253, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 24, 137, 183, 232, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 240, 90, 43, 23, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 96, 141, 177, 231, 246, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 192, 133, 239, 12, 165, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 128, 57, 91, 129, 114, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 63, 144, 13, 121, 220, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 118, 162, 135, 186, 156, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 156, 88, 76, 73, 31, 242, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 24, 118, 251, 220, 56, 117, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 240, 156, 210, 161, 56, 148, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 96, 33, 58, 82, 54, 202, 201, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 192, 77, 69, 54, 31, 230, 225, 253, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 128, 9, 181, 30, 56, 253, 210, 234, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 95, 18, 51, 49, 228, 61, 44, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 182, 183, 254, 235, 233, 106, 186, 247, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 28, 45, 243, 55, 35, 45, 72, 173, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 24, 195, 127, 47, 96, 195, 209, 196, 252, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 240, 158, 253, 218, 193, 161, 49, 176, 223, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 96, 53, 232, 141, 146, 81, 240, 225, 188, 254, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 192, 21, 18, 139, 185, 47, 99, 211, 96, 243, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 128, 217, 180, 110, 63, 221, 223, 65, 200, 129, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 127, 16, 83, 122, 164, 190, 146, 210, 17, 251, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 246, 164, 62, 199, 108, 114, 187, 57, 178, 206, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 156, 113, 114, 200, 63, 120, 82, 65, 246, 18, 254, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 24, 112, 120, 212, 125, 178, 56, 141, 158, 189, 236, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 240, 96, 180, 76, 234, 248, 54, 132, 49, 104, 63, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 96, 201, 11, 255, 38, 185, 37, 42, 239, 17, 122, 248, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 192, 221, 117, 246, 133, 59, 121, 165, 87, 179, 196, 180, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 128, 169, 154, 160, 59, 83, 188, 118, 108, 1, 175, 15, 253, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 159, 10, 70, 84, 64, 91, 163, 60, 14, 214, 156, 226, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 54, 106, 188, 74, 131, 144, 97, 94, 142, 92, 32, 218, 254, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 28, 38, 92, 235, 32, 165, 207, 175, 143, 157, 67, 133, 244, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 24, 125, 153, 49, 73, 115, 28, 222, 156, 39, 164, 52, 141, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 240, 226, 254, 239, 219, 128, 28, 173, 32, 140, 105, 14, 132, 251, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 96, 221, 244, 95, 151, 8, 29, 195, 70, 121, 31, 144, 40, 211, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 192, 165, 144, 191, 233, 85, 34, 159, 195, 188, 58, 161, 149, 63, 254, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 128, 121, 166, 123, 33, 91, 87, 55, 164, 95, 75, 76, 216, 123, 238, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 191, 128, 212, 78, 143, 105, 41, 106, 188, 241, 250, 114, 214, 80, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 118, 7, 77, 20, 153, 31, 158, 37, 92, 113, 205, 125, 96, 40, 249, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 156, 74, 2, 203, 250, 59, 45, 120, 153, 109, 6, 234, 196, 147, 187, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 24, 234, 22, 238, 203, 87, 196, 177, 254, 71, 64, 36, 177, 197, 83, 253,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 240, 36, 229, 76, 247, 109, 171, 241, 242, 207, 130, 106, 235, 184, 69,
        229, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 96, 113, 243, 0, 169, 75, 178, 112, 125, 31, 28, 41, 50, 57, 185, 244,
        254, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 192, 109, 130, 9, 154, 244, 246, 102, 230, 58, 25, 155, 245, 59, 60, 143,
        245, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 128, 73, 24, 95, 4, 142, 165, 5, 0, 77, 252, 14, 152, 87, 90, 152, 151,
        255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 223, 242, 182, 43, 140, 119, 56, 0, 2, 219, 149, 240, 107, 135, 243,
        235, 251, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 182, 124, 37, 181, 121, 171, 52, 2, 20, 142, 218, 101, 55, 74, 131,
        55, 215, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 28, 223, 118, 19, 193, 178, 14, 22, 200, 140, 137, 250, 41, 230, 32,
        43, 104, 254, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 24, 183, 164, 194, 138, 251, 146, 220, 208, 127, 95, 201, 163, 253,
        72, 175, 17, 240, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 240, 38, 111, 154, 107, 211, 189, 157, 40, 254, 186, 221, 101, 232,
        217, 216, 176, 96, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 96, 133, 87, 8, 52, 66, 106, 41, 150, 237, 77, 169, 250, 19, 131, 120,
        232, 198, 249, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 192, 53, 107, 83, 8, 150, 38, 158, 221, 71, 11, 157, 202, 199, 30,
        181, 20, 197, 193, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 128, 25, 48, 66, 83, 220, 129, 45, 168, 206, 112, 34, 234, 205, 51,
        19, 207, 178, 145, 253, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 255, 224, 149, 64, 155, 18, 199, 145, 18, 104, 88, 37, 11, 6, 192,
        22, 252, 176, 231, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 246, 201, 218, 133, 16, 186, 198, 177, 185, 16, 116, 117, 111, 60,
        128, 227, 216, 233, 12, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 156, 227, 139, 58, 165, 68, 195, 241, 64, 167, 136, 150, 90, 92, 2,
        227, 120, 34, 129, 246, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 24, 228, 118, 73, 116, 174, 160, 113, 137, 136, 86, 225, 137, 155,
        23, 222, 184, 88, 11, 161, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 240, 232, 164, 222, 138, 208, 70, 112, 94, 85, 97, 205, 98, 19,
        236, 172, 56, 119, 113, 74, 252, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 96, 25, 113, 178, 108, 37, 196, 98, 176, 85, 205, 5, 220, 193, 56,
        193, 54, 168, 110, 232, 218, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 192, 253, 106, 248, 62, 118, 169, 219, 227, 88, 5, 58, 152, 146,
        55, 140, 35, 146, 82, 20, 141, 254, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 128, 233, 45, 180, 117, 158, 158, 148, 230, 120, 53, 68, 242, 185,
        43, 122, 99, 181, 57, 203, 130, 241, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 31, 203, 9, 153, 48, 50, 206, 1, 185, 22, 170, 118, 67, 181,
        197, 226, 21, 65, 240, 27, 111, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 54, 239, 97, 250, 229, 245, 13, 18, 58, 227, 164, 162, 162, 20,
        185, 219, 218, 138, 98, 23, 87, 250, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 28, 88, 211, 199, 251, 154, 139, 180, 68, 224, 112, 90, 90, 206,
        58, 149, 140, 108, 217, 233, 102, 199, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 24, 113, 65, 206, 213, 13, 116, 13, 175, 194, 104, 136, 135, 15,
        76, 212, 125, 61, 126, 34, 5, 202, 253,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 240, 106, 142, 14, 90, 138, 136, 134, 214, 154, 23, 84, 75, 155,
        248, 74, 234, 102, 238, 88, 51, 228, 233,
    ]),
];

/// The wrapper of `ethnum::I256`, used to implement the `BorshSerialize` and `BorshDeserialize` traits.
#[derive(Clone, Copy, Default, Eq, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct i256(pub ethnum::I256);

impl i256 {
    /// The additive identity for this integer type, i.e. `0`.
    pub const ZERO: Self = Self(ethnum::I256([0; 2]));

    /// The multiplicative identity for this integer type, i.e. `1`.
    pub const ONE: Self = Self(ethnum::I256::new(1));

    /// The multiplicative inverse for this integer type, i.e. `-1`.
    pub const MINUS_ONE: Self = Self(ethnum::I256::new(-1));

    /// Creates a new 256-bit integer value from a primitive `i128` integer.
    #[inline]
    pub const fn new(value: i128) -> Self {
        Self(ethnum::I256::new(value))
    }

    /// Returns a new [`i256`] from two `i128`.
    pub fn from_words(hi: i128, lo: i128) -> Self {
        Self(ethnum::I256::from_words(hi, lo))
    }

    pub fn from_str_radix(
        src: &str,
        radix: u32,
    ) -> std::result::Result<Self, std::num::ParseIntError> {
        ethnum::I256::from_str_radix(src, radix).map(Self)
    }

    #[inline]
    pub const fn to_le_bytes(&self) -> [u8; 32] {
        self.0.to_le_bytes()
    }

    #[inline]
    pub const fn to_be_bytes(&self) -> [u8; 32] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub const fn from_be_bytes(bytes: [u8; 32]) -> Self {
        Self(ethnum::I256::from_be_bytes(bytes))
    }

    #[inline]
    pub const fn from_le_bytes(bytes: [u8; 32]) -> Self {
        Self(ethnum::I256::from_le_bytes(bytes))
    }

    #[inline]
    pub const fn is_positive(self) -> bool {
        self.0.is_positive()
    }

    #[inline]
    pub const fn is_negative(self) -> bool {
        self.0.is_negative()
    }

    #[inline]
    pub fn saturating_abs(self) -> Self {
        Self(self.0.saturating_abs())
    }

    #[inline]
    pub fn leading_zeros(self) -> u32 {
        self.0.leading_zeros()
    }

    /// Cast to a primitive `i8`.
    #[inline]
    pub const fn as_i8(self) -> i8 {
        self.0.as_i8()
    }

    /// Cast to a primitive `i16`.
    #[inline]
    pub const fn as_i16(self) -> i16 {
        self.0.as_i16()
    }

    /// Cast to a primitive `i32`.
    #[inline]
    pub const fn as_i32(self) -> i32 {
        self.0.as_i32()
    }

    /// Cast to a primitive `i64`.
    #[inline]
    pub const fn as_i64(self) -> i64 {
        self.0.as_i64()
    }

    /// Cast to a primitive `i128`.
    #[inline]
    pub const fn as_i128(self) -> i128 {
        self.0.as_i128()
    }

    /// Cast to a primitive `u8`.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self.0.as_u8()
    }

    /// Cast to a primitive `u16`.
    #[inline]
    pub const fn as_u16(self) -> u16 {
        self.0.as_u16()
    }

    /// Cast to a primitive `u32`.
    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0.as_u32()
    }

    /// Cast to a primitive `u64`.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0.as_u64()
    }

    /// Cast to a primitive `u128`.
    #[inline]
    pub const fn as_u128(self) -> u128 {
        self.0.as_u128()
    }

    /// Cast to a primitive `u256`.
    #[inline]
    pub const fn as_u256(self) -> u256 {
        self.0.as_u256()
    }

    /// Cast to a primitive `isize`.
    #[inline]
    pub const fn as_isize(self) -> isize {
        self.0.as_isize()
    }

    /// Cast to a primitive `usize`.
    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0.as_usize()
    }

    /// Cast to a primitive `f32`.
    #[inline]
    pub fn as_f32(self) -> f32 {
        self.0.as_f32()
    }

    /// Cast to a primitive `f64`.
    #[inline]
    pub fn as_f64(self) -> f64 {
        self.0.as_f64()
    }

    /// Get the low 128-bit word for this signed integer.
    #[inline]
    pub fn low(&self) -> &i128 {
        self.0.low()
    }

    /// Get the high 128-bit word for this signed integer.
    #[inline]
    pub fn high(&self) -> &i128 {
        self.0.high()
    }

    #[allow(unused_attributes)]
    #[inline]
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    #[inline]
    pub fn checked_neg(self) -> Option<Self> {
        self.0.checked_neg().map(Self)
    }

    #[inline]
    pub fn pow(self, exp: u32) -> Self {
        Self(self.0.pow(exp))
    }
}

impl std::fmt::Debug for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Neg for i256 {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self::Output {
        Self(self.0.checked_neg().expect("i256 overflow"))
    }
}

impl AddAssign for i256 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl SubAssign for i256 {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl MulAssign for i256 {
    fn mul_assign(&mut self, rhs: Self) {
        self.0 *= rhs.0;
    }
}

impl DivAssign for i256 {
    fn div_assign(&mut self, rhs: Self) {
        self.0 /= rhs.0;
    }
}

impl Add for i256 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl Sub for i256 {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Mul for i256 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        Self(self.0 * rhs.0)
    }
}

impl Div for i256 {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        Self(self.0 / rhs.0)
    }
}

impl Rem for i256 {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self::Output {
        Self(self.0 % rhs.0)
    }
}

macro_rules! impl_from {
    ($($t:ty),* $(,)?) => {$(
        impl From<$t> for i256 {
            #[inline]
            fn from(value: $t) -> Self {
                i256(value.as_i256())
            }
        }
    )*};
}

impl_from! {
    bool,
    i8, i16, i32, i64, i128,
    u8, u16, u32, u64, u128,
}

impl TryFrom<u256> for i256 {
    type Error = TryFromIntError;

    fn try_from(value: u256) -> std::result::Result<Self, Self::Error> {
        let i256_value = ethnum::i256::try_from(value)?;
        Ok(i256(i256_value))
    }
}

impl BorshSerialize for i256 {
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        BorshSerialize::serialize(&self.0 .0, writer)
    }
}

impl BorshDeserialize for i256 {
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        let value: [i128; 2] = BorshDeserialize::deserialize_reader(reader)?;
        Ok(Self(ethnum::I256(value)))
    }
}

impl Marshal for i256 {
    fn marshal(&self, scratch: &mut [u8]) {
        self.0.marshal(scratch);
    }
}

macro_rules! impl_into_float {
    ($($t:ty => $f:ident),* $(,)?) => {$(
        impl From<i256> for $t {
            #[inline]
            fn from(x: i256) -> $t {
                x.0.$f()
            }
        }
    )*};
}

impl_into_float! {
    f32 => as_f32, f64 => as_f64,
}

impl core::hash::Hash for i256 {
    #[inline]
    fn hash<H>(&self, hasher: &mut H)
    where H: core::hash::Hasher {
        core::hash::Hash::hash(&self.0, hasher);
    }
}

impl PartialEq for i256 {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialEq<i128> for i256 {
    #[inline]
    fn eq(&self, other: &i128) -> bool {
        *self == i256::new(*other)
    }
}

impl PartialEq<i256> for i128 {
    #[inline]
    fn eq(&self, other: &i256) -> bool {
        i256::new(*self) == *other
    }
}

impl PartialOrd for i256 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd<i128> for i256 {
    #[inline]
    fn partial_cmp(&self, rhs: &i128) -> Option<core::cmp::Ordering> {
        Some(self.cmp(&i256::new(*rhs)))
    }
}

impl PartialOrd<i256> for i128 {
    #[inline]
    fn partial_cmp(&self, rhs: &i256) -> Option<core::cmp::Ordering> {
        Some(i256::new(*self).cmp(rhs))
    }
}

impl Ord for i256 {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

pub type DecimalView<F, T> = ComputeView<DecimalConvert<F, T>, CoreDecimal<F>, CoreDecimal<T>>;
pub type DecimalF64View<F> =
    ComputeView<DecimalConvert<F, F64>, CoreScalarDecimal<F>, NumberType<F64>>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecimalConvert<F, T>(std::marker::PhantomData<(F, T)>);

impl<F, T> Compute<CoreDecimal<F>, CoreDecimal<T>> for DecimalConvert<F, T>
where
    F: Decimal,
    T: Decimal,
{
    #[inline]
    fn compute<'a>(
        value: <CoreDecimal<F> as AccessType>::ScalarRef<'a>,
    ) -> <CoreDecimal<T> as AccessType>::ScalarRef<'a> {
        value.as_decimal::<T>()
    }

    fn compute_domain(domain: &SimpleDomain<F>) -> SimpleDomain<T> {
        SimpleDomain {
            min: domain.min.as_decimal::<T>(),
            max: domain.max.as_decimal::<T>(),
        }
    }
}

impl<F> Compute<CoreScalarDecimal<F>, NumberType<F64>> for DecimalConvert<F, F64>
where F: Decimal
{
    #[inline]
    fn compute<'a>(
        value: <CoreScalarDecimal<F> as AccessType>::ScalarRef<'a>,
    ) -> <NumberType<F64> as AccessType>::ScalarRef<'a> {
        value.0.to_float64(value.1.scale()).into()
    }

    fn compute_domain(domain: &SimpleDomain<(F, DecimalSize)>) -> SimpleDomain<F64> {
        let min = domain.min.0.to_float64(domain.min.1.scale());
        let max = domain.max.0.to_float64(domain.max.1.scale());

        SimpleDomain {
            min: min.into(),
            max: max.into(),
        }
    }
}

pub type Decimal64AsF64Type = DecimalF64View<i64>;
pub type Decimal128AsF64Type = DecimalF64View<i128>;
pub type Decimal256AsF64Type = DecimalF64View<i256>;

macro_rules! decimal_convert_type {
    ($from:ty, $to:ty, $alias:ident, $compat:ident) => {
        pub type $alias = DecimalView<$from, $to>;
        pub type $compat = DecimalConvert<$from, $to>;
    };
}

decimal_convert_type!(i64, i128, Decimal64As128Type, I64ToI128);
decimal_convert_type!(i128, i64, Decimal128As64Type, I128ToI64);
decimal_convert_type!(i64, i256, Decimal64As256Type, I64ToI256);
decimal_convert_type!(i128, i256, Decimal128As256Type, I128ToI256);
decimal_convert_type!(i256, i128, Decimal256As128Type, I256ToI128);
decimal_convert_type!(i256, i64, Decimal256As64Type, I256ToI64);

#[cfg(test)]
mod tests {
    use super::ValueType;
    use super::*;

    #[test]
    fn test_decimal_cast() {
        let test_values = vec![
            0i128,
            1i128,
            -1i128,
            i128::MAX,
            i128::MIN,
            123456789i128,
            -987654321i128,
        ];

        let size = DecimalSize::new_unchecked(38, 10);
        let col_128 = Decimal128Type::from_data_with_size(test_values.clone(), Some(size));

        let downcast_col = Decimal128As256Type::try_downcast_column(&col_128).unwrap();
        let builder: Vec<i256> = Decimal128As256Type::iter_column(&downcast_col).collect();

        let col_256 = i256::upcast_column(Decimal256Type::build_column(builder), size);

        let buffer = Decimal256Type::try_downcast_column(&col_256).unwrap();

        for (got, want) in buffer.iter().zip(&test_values) {
            assert_eq!(*got, i256::from(*want));
        }

        let downcast_col = Decimal256As128Type::try_downcast_column(&col_256).unwrap();
        let builder: Vec<i128> = Decimal256As128Type::iter_column(&downcast_col).collect();

        let col_128 = i128::upcast_column(Decimal128Type::build_column(builder), size);

        let buffer = Decimal128Type::try_downcast_column(&col_128).unwrap();

        for (got, want) in buffer.iter().zip(&test_values) {
            assert_eq!(got, want);
        }
    }

    #[test]
    fn test_min_max_for_precision() {
        assert_eq!(i128::max_for_precision(1), 9);
        assert_eq!(i128::max_for_precision(2), 99);
        assert_eq!(i128::max_for_precision(38), i128::DECIMAL_MAX);

        assert_eq!(i128::min_for_precision(1), -9);
        assert_eq!(i128::min_for_precision(2), -99);
        assert_eq!(i128::min_for_precision(38), i128::DECIMAL_MIN);
    }
}
