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

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_exception::ErrorCode;
use common_exception::Result;
use enum_as_inner::EnumAsInner;
use ethnum::i256;
use itertools::Itertools;
use num_traits::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::SimpleDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalType<T: Decimal>(PhantomData<T>);

pub type Decimal128Type = DecimalType<i128>;
pub type Decimal256Type = DecimalType<i256>;

impl<Num: Decimal> ValueType for DecimalType<Num> {
    type Scalar = Num;
    type ScalarRef<'a> = Num;
    type Column = Buffer<Num>;
    type Domain = SimpleDomain<Num>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, Num>>;
    type ColumnBuilder = Vec<Num>;

    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        Num::try_downcast_scalar(scalar.as_decimal()?)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        let down_col = Num::try_downcast_column(col);
        if let Some(col) = down_col {
            Some(col.0)
        } else {
            None
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        Num::try_downcast_domain(domain.as_decimal()?)
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        Num::try_downcast_builder(builder)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Num::upcast_scalar(scalar, Num::default_decimal_size())
    }

    fn upcast_column(col: Self::Column) -> Column {
        Num::upcast_column(col, Num::default_decimal_size())
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Num::upcast_domain(domain, Num::default_decimal_size())
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index).cloned()
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        *col.get_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter().cloned()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        buffer_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Num::default())
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.extend_from_slice(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }
}

impl<Num: Decimal> ArgType for DecimalType<Num> {
    fn data_type() -> DataType {
        Num::data_type()
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: Num::MIN,
            max: Num::MAX,
        }
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.into()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum DecimalDataType {
    Decimal128(DecimalSize),
    Decimal256(DecimalSize),
}

#[derive(Clone, Copy, PartialEq, Eq, EnumAsInner, Serialize, Deserialize)]
pub enum DecimalScalar {
    Decimal128(i128, DecimalSize),
    Decimal256(i256, DecimalSize),
}

impl DecimalScalar {
    pub fn to_float64(&self) -> f64 {
        match self {
            DecimalScalar::Decimal128(v, size) => i128::to_float64(*v, size.scale),
            DecimalScalar::Decimal256(v, size) => i256::to_float64(*v, size.scale),
        }
    }
    pub fn is_positive(&self) -> bool {
        match self {
            DecimalScalar::Decimal128(v, _) => i128::is_positive(*v),
            DecimalScalar::Decimal256(v, _) => i256::is_positive(*v),
        }
    }
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum DecimalColumn {
    Decimal128(Buffer<i128>, DecimalSize),
    Decimal256(Buffer<i256>, DecimalSize),
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum DecimalColumnBuilder {
    Decimal128(Vec<i128>, DecimalSize),
    Decimal256(Vec<i256>, DecimalSize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum DecimalDomain {
    Decimal128(SimpleDomain<i128>, DecimalSize),
    Decimal256(SimpleDomain<i256>, DecimalSize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DecimalSize {
    pub precision: u8,
    pub scale: u8,
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
    + PartialOrd
    + Ord
    + Sync
    + Send
    + 'static
{
    fn zero() -> Self;
    fn one() -> Self;
    fn minus_one() -> Self;

    // 10**scale
    fn e(n: u32) -> Self;

    fn mem_size() -> usize;

    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn checked_sub(self, rhs: Self) -> Option<Self>;
    fn checked_div(self, rhs: Self) -> Option<Self>;
    fn checked_mul(self, rhs: Self) -> Option<Self>;

    fn max_of_max_precision() -> Self;

    fn min_for_precision(precision: u8) -> Self;
    fn max_for_precision(precision: u8) -> Self;

    fn default_decimal_size() -> DecimalSize;

    fn from_float(value: f64) -> Self;
    fn from_u64(value: u64) -> Self;
    fn from_i64(value: i64) -> Self;
    fn de_binary(bytes: &mut &[u8]) -> Self;

    fn to_float64(self, scale: u8) -> f64;

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)>;
    fn try_downcast_builder<'a>(builder: &'a mut ColumnBuilder) -> Option<&'a mut Vec<Self>>;

    fn try_downcast_scalar(scalar: &DecimalScalar) -> Option<Self>;
    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>>;

    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar;
    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column;
    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain;
    fn data_type() -> DataType;
    const MIN: Self;
    const MAX: Self;
    const MAX_VALUE: Self;

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn;

    fn to_column(value: Vec<Self>, size: DecimalSize) -> DecimalColumn {
        Self::to_column_from_buffer(value.into(), size)
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar;

    fn with_size(&self, size: DecimalSize) -> Option<Self> {
        let multiplier = Self::e(size.scale as u32);
        let min_for_precision = Self::min_for_precision(size.precision);
        let max_for_precision = Self::max_for_precision(size.precision);
        self.checked_mul(multiplier).and_then(|v| {
            if v > max_for_precision || v < min_for_precision {
                None
            } else {
                Some(v)
            }
        })
    }
}

impl Decimal for i128 {
    const MAX_VALUE: i128 = 170141183460469231731687303715884105727_i128;
    fn zero() -> Self {
        0_i128
    }

    fn one() -> Self {
        1_i128
    }

    fn minus_one() -> Self {
        -1_i128
    }

    fn e(n: u32) -> Self {
        10_i128.pow(n)
    }

    fn mem_size() -> usize {
        16
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

    fn max_of_max_precision() -> Self {
        Self::MAX_VALUE
    }

    fn min_for_precision(to_precision: u8) -> Self {
        9_i128
            .saturating_pow(1 + to_precision as u32)
            .saturating_neg()
    }

    fn max_for_precision(to_precision: u8) -> Self {
        9_i128.saturating_pow(1 + to_precision as u32)
    }

    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: MAX_DECIMAL128_PRECISION,
            scale: 0,
        }
    }

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal128(value, size)
    }

    fn from_float(value: f64) -> Self {
        value.to_i128().unwrap()
    }

    fn from_u64(value: u64) -> Self {
        value.to_i128().unwrap()
    }

    fn from_i64(value: i64) -> Self {
        value.to_i128().unwrap()
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i128::from_le_bytes(bs)
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self as f64 / div
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar {
        DecimalScalar::Decimal128(self, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal128(c, size) => Some((c.clone(), *size)),
            DecimalColumn::Decimal256(_, _) => None,
        }
    }

    fn try_downcast_builder<'a>(builder: &'a mut ColumnBuilder) -> Option<&'a mut Vec<Self>> {
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

    fn data_type() -> DataType {
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: MAX_DECIMAL128_PRECISION,
            scale: 0,
        }))
    }

    const MIN: i128 = -99999999999999999999999999999999999999i128;

    const MAX: i128 = 99999999999999999999999999999999999999i128;
}

impl Decimal for i256 {
    const MAX_VALUE: i256 = ethnum::int!(
        "57896044618658097711785492504343953926634992332820282019728792003956564819967"
    );
    fn zero() -> Self {
        i256::ZERO
    }

    fn one() -> Self {
        i256::ONE
    }

    fn minus_one() -> Self {
        i256::MINUS_ONE
    }

    fn e(n: u32) -> Self {
        (i256::ONE * 10).pow(n)
    }

    fn mem_size() -> usize {
        32
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

    fn max_of_max_precision() -> Self {
        Self::MAX_VALUE
    }

    fn min_for_precision(to_precision: u8) -> Self {
        (i256::ONE * 9)
            .saturating_pow(1 + to_precision as u32)
            .saturating_neg()
    }

    fn max_for_precision(to_precision: u8) -> Self {
        (i256::ONE * 9).saturating_pow(1 + to_precision as u32)
    }

    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: MAX_DECIMAL256_PRECISION,
            scale: 0,
        }
    }

    fn from_float(value: f64) -> Self {
        i256::from(value.to_i128().unwrap())
    }

    fn from_u64(value: u64) -> Self {
        i256::from(value.to_i128().unwrap())
    }

    fn from_i64(value: i64) -> Self {
        i256::from(value.to_i128().unwrap())
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i256::from_le_bytes(bs)
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self.as_f64() / div
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

    fn try_downcast_builder<'a>(builder: &'a mut ColumnBuilder) -> Option<&'a mut Vec<Self>> {
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

    fn data_type() -> DataType {
        DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
            precision: MAX_DECIMAL256_PRECISION,
            scale: 0,
        }))
    }

    const MIN: i256 = ethnum::int!(
        "-9999999999999999999999999999999999999999999999999999999999999999999999999999"
    );
    const MAX: i256 = ethnum::int!(
        "9999999999999999999999999999999999999999999999999999999999999999999999999999"
    );
    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal256(value, size)
    }
}

pub static MAX_DECIMAL128_PRECISION: u8 = 38;
pub static MAX_DECIMAL256_PRECISION: u8 = 76;

impl DecimalDataType {
    pub fn from_size(size: DecimalSize) -> Result<DecimalDataType> {
        if size.precision < 1 || size.precision > MAX_DECIMAL256_PRECISION {
            return Err(ErrorCode::Overflow(format!(
                "Decimal precision must be between 1 and {}",
                MAX_DECIMAL256_PRECISION
            )));
        }

        if size.scale > size.precision {
            return Err(ErrorCode::Overflow(format!(
                "Decimal scale must be between 0 and precision {}",
                size.precision
            )));
        }
        if size.precision <= MAX_DECIMAL128_PRECISION {
            Ok(DecimalDataType::Decimal128(size))
        } else {
            Ok(DecimalDataType::Decimal256(size))
        }
    }

    pub fn default_scalar(&self) -> DecimalScalar {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => DecimalScalar::DECIMAL_TYPE(0.into(), *size),
        })
    }

    pub fn size(&self) -> DecimalSize {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => *size,
        })
    }

    pub fn scale(&self) -> u8 {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.scale,
        })
    }

    pub fn precision(&self) -> u8 {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.precision,
        })
    }

    pub fn leading_digits(&self) -> u8 {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.precision - size.scale,
        })
    }

    pub fn max_precision(&self) -> u8 {
        match self {
            DecimalDataType::Decimal128(_) => MAX_DECIMAL128_PRECISION,
            DecimalDataType::Decimal256(_) => MAX_DECIMAL256_PRECISION,
        }
    }

    pub fn max_result_precision(&self, other: &Self) -> u8 {
        if matches!(other, DecimalDataType::Decimal128(_)) {
            return self.max_precision();
        }
        other.max_precision()
    }

    pub fn binary_result_type(
        a: &Self,
        b: &Self,
        is_multiply: bool,
        is_divide: bool,
        is_plus_minus: bool,
    ) -> Result<Self> {
        let mut scale = a.scale().max(b.scale());
        let mut precision = a.max_result_precision(b);

        let multiply_precision = a.precision() + b.precision();
        let divide_precision = a.precision() + b.scale();

        if is_multiply {
            scale = a.scale() + b.scale();
            precision = precision.min(multiply_precision);
        } else if is_divide {
            scale = a.scale();
            precision = precision.min(divide_precision);
        } else if is_plus_minus {
            scale = std::cmp::max(a.scale(), b.scale());
            // for addition/subtraction, we add 1 to the width to ensure we don't overflow
            let plus_min_precision = a.leading_digits().max(b.leading_digits()) + scale + 1;
            precision = precision.min(plus_min_precision);
        }

        // if the args both are Decimal128, we need to clamp the precision to 38
        if a.precision() <= MAX_DECIMAL128_PRECISION && b.precision() <= MAX_DECIMAL128_PRECISION {
            precision = precision.min(MAX_DECIMAL128_PRECISION);
        }

        Self::from_size(DecimalSize { precision, scale })
    }
}

impl DecimalScalar {
    pub fn domain(&self) -> DecimalDomain {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalScalar::DECIMAL_TYPE(num, size) => DecimalDomain::DECIMAL_TYPE(
                SimpleDomain {
                    min: *num,
                    max: *num,
                },
                *size
            ),
        })
    }
}

impl DecimalColumn {
    pub fn len(&self) -> usize {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, _) => col.len(),
        })
    }

    pub fn index(&self, index: usize) -> Option<DecimalScalar> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                Some(DecimalScalar::DECIMAL_TYPE(col.get(index).cloned()?, *size)),
        })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> DecimalScalar {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
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

        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
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
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
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
}

impl DecimalColumnBuilder {
    pub fn from_column(col: DecimalColumn) -> Self {
        crate::with_decimal_type!(|DECIMAL_TYPE| match col {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(buffer_into_mut(col), size),
        })
    }

    pub fn repeat(scalar: DecimalScalar, n: usize) -> DecimalColumnBuilder {
        crate::with_decimal_type!(|DECIMAL_TYPE| match scalar {
            DecimalScalar::DECIMAL_TYPE(num, size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(vec![num; n], size),
        })
    }

    pub fn len(&self) -> usize {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(col, _) => col.len(),
        })
    }

    pub fn with_capacity(ty: &DecimalDataType, capacity: usize) -> Self {
        crate::with_decimal_type!(|DECIMAL_TYPE| match ty {
            DecimalDataType::DECIMAL_TYPE(size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(Vec::with_capacity(capacity), *size),
        })
    }

    pub fn push(&mut self, item: DecimalScalar) {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, item) {
            (
                DecimalColumnBuilder::DECIMAL_TYPE(builder, builder_size),
                DecimalScalar::DECIMAL_TYPE(value, value_size),
            ) => {
                debug_assert_eq!(*builder_size, value_size);
                builder.push(value)
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        })
    }

    pub fn push_default(&mut self) {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, _) => builder.push(0.into()),
        })
    }

    pub fn append_column(&mut self, other: &DecimalColumn) {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
            (
                DecimalColumnBuilder::DECIMAL_TYPE(builder, builder_size),
                DecimalColumn::DECIMAL_TYPE(other, other_size),
            ) => {
                debug_assert_eq!(builder_size, other_size);
                builder.extend_from_slice(other);
            }
            (this, other) => unreachable!("unable append {other:?} onto {this:?}"),
        })
    }

    pub fn build(self) -> DecimalColumn {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) =>
                DecimalColumn::DECIMAL_TYPE(builder.into(), size),
        })
    }

    pub fn build_scalar(self) -> DecimalScalar {
        assert_eq!(self.len(), 1);

        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) =>
                DecimalScalar::DECIMAL_TYPE(builder[0], size),
        })
    }

    pub fn pop(&mut self) -> Option<DecimalScalar> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) => {
                builder
                    .pop()
                    .map(|num| DecimalScalar::DECIMAL_TYPE(num, *size))
            }
        })
    }
}

impl PartialOrd for DecimalScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
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
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
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
            $t = [Decimal128, Decimal256],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_decimal_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Decimal128 => i128, Decimal256 => i256
            ],
            $($tail)*
        }
    }
}
