// Copyright 2022 Datafuse Labs.
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
use crate::utils::arrow::buffer_into_mut;
use crate::Column;

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

pub trait Decimal: Sized {
    fn one() -> Self;
    // 10**scale
    fn e(n: u32) -> Self;

    fn min_for_precision(precision: u8) -> Self;
    fn max_for_precision(precision: u8) -> Self;

    fn from_float(value: f64) -> Self;

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)>;

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn;

    fn to_column(value: Vec<Self>, size: DecimalSize) -> DecimalColumn {
        Self::to_column_from_buffer(value.into(), size)
    }
}

impl Decimal for i128 {
    fn one() -> Self {
        1_i128
    }
    fn e(n: u32) -> Self {
        10_i128.pow(n)
    }

    fn min_for_precision(to_precision: u8) -> Self {
        9_i128
            .saturating_pow(1 + to_precision as u32)
            .saturating_neg()
    }

    fn max_for_precision(to_precision: u8) -> Self {
        9_i128.saturating_pow(1 + to_precision as u32)
    }

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal128(value, size)
    }

    fn from_float(value: f64) -> Self {
        value.to_i128().unwrap()
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal128(c, size) => Some((c.clone(), *size)),
            DecimalColumn::Decimal256(_, _) => None,
        }
    }
}

impl Decimal for i256 {
    fn one() -> Self {
        i256::ONE
    }

    fn e(n: u32) -> Self {
        (i256::ONE * 10).pow(n)
    }

    fn min_for_precision(to_precision: u8) -> Self {
        (i256::ONE * 9)
            .saturating_pow(1 + to_precision as u32)
            .saturating_neg()
    }

    fn max_for_precision(to_precision: u8) -> Self {
        (i256::ONE * 9).saturating_pow(1 + to_precision as u32)
    }

    fn from_float(value: f64) -> Self {
        i256::from(value.to_i128().unwrap())
    }

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal256(value, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal128(_, _) => None,
            DecimalColumn::Decimal256(c, size) => Some((c.clone(), *size)),
        }
    }
}

static MAX_DECIMAL128_PRECISION: u8 = 38;
static MAX_DECIMAL256_PRECISION: u8 = 76;

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

        // for addition/subtraction, we add 1 to the width to ensure we don't overflow
        let plus_min_precision = a.leading_digits().max(b.leading_digits()) - scale + 1;

        if is_multiply {
            scale = a.scale() + b.scale();
            precision = precision.min(multiply_precision);
        } else if is_divide {
            scale = a.scale();
            precision = precision.min(divide_precision);
        } else if is_plus_minus {
            scale = std::cmp::max(a.scale(), b.scale());
            precision = precision.min(plus_min_precision);
        }

        Self::from_size(DecimalSize { precision, scale })
    }

    // Decimal X Number or Nunmber X Decimal
    pub fn binary_upgrade_to_max_precision(&self) -> Result<DecimalDataType> {
        let scale = self.scale();
        let precision = self.max_precision();
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
    /// Assumes that the `index` is not out of range.
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
                    col.clone().slice(range.start, range.end - range.start),
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
