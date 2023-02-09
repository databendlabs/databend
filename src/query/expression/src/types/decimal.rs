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
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::Number;
use super::SimpleDomain;
use crate::utils::arrow::buffer_into_mut;

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

pub trait Decimal {
    fn one() -> Self;
    // 10**scale
    fn e(n: u32) -> Self;
}

impl Decimal for i128 {
    fn one() -> Self {
        1_i128
    }
    fn e(n: u32) -> Self {
        10_i128.pow(n)
    }
}

impl Decimal for i256 {
    fn one() -> Self {
        i256::ONE
    }

    fn e(n: u32) -> Self {
        (i256::ONE * 10).pow(n)
    }
}

static MAX_DECIMAL128_PRECISION: u8 = 38;
static MAX_DECIMAL256_PRECISION: u8 = 76;

impl DecimalDataType {
    pub fn from_size(size: DecimalSize) -> Result<DecimalDataType> {
        if size.precision < 1 || size.precision > MAX_DECIMAL256_PRECISION {
            return Err(ErrorCode::Overflow(format!(
                "Decimal precision must be between 1 and {}",
                MAX_DECIMAL128_PRECISION
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

    pub fn max_precision(&self) -> u8 {
        match self {
            DecimalDataType::Decimal128(_) => MAX_DECIMAL128_PRECISION,
            DecimalDataType::Decimal256(_) => MAX_DECIMAL256_PRECISION,
        }
    }

    pub fn max_result_precision(&self, other: &Self) -> u8 {
        if matches!(self, DecimalDataType::Decimal128(_)) {
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
        let mut scale = 0;
        if is_multiply {
            scale = a.scale() + b.scale();
        } else if is_divide {
            scale = a.scale();
        } else if is_plus_minus {
            scale = std::cmp::max(a.scale(), b.scale());
        }
        let precision = a.max_result_precision(b);

        Self::from_size(DecimalSize { precision, scale })
    }

    pub fn convert_from(&self, col: &DecimalColumn) -> Result<DecimalColumn> {
        match col {
            DecimalColumn::Decimal128(buffer, from) => {
                match self {
                    DecimalDataType::Decimal128(size) => {
                        // faster path
                        if from.scale == size.scale && from.precision <= size.precision {
                            return Ok(DecimalColumn::Decimal128(buffer.clone(), *size));
                        }
                        if from.scale > size.scale {
                            let factor = i128::e((from.scale - size.scale) as u32);
                            let mut new_buffer = Vec::with_capacity(buffer.len());
                            for x in buffer.iter() {
                                new_buffer.push(x.checked_div(factor).ok_or_else(|| {
                                    ErrorCode::Overflow(format!(
                                        "Decimal overflow when converting from {:?} to {:?}",
                                        from, size
                                    ))
                                })?);
                            }
                            Ok(DecimalColumn::Decimal128(new_buffer.into(), *size))
                        } else {
                            let factor = i128::e((size.scale - from.scale) as u32);
                            let mut new_buffer = Vec::with_capacity(buffer.len());
                            for x in buffer.iter() {
                                new_buffer.push(x.checked_mul(factor).ok_or_else(|| {
                                    ErrorCode::Overflow(format!(
                                        "Decimal overflow when converting from {:?} to {:?}",
                                        from, size
                                    ))
                                })?);
                            }
                            Ok(DecimalColumn::Decimal128(new_buffer.into(), *size))
                        }
                    }
                    DecimalDataType::Decimal256(_) => todo!(),
                }
            }
            DecimalColumn::Decimal256(_buffer, _from) => match self {
                DecimalDataType::Decimal128(_) => todo!(),
                DecimalDataType::Decimal256(_) => todo!(),
            },
        }
    }

    pub fn from_integer<T: Number + AsPrimitive<i128>>(
        &self,
        from: Buffer<T>,
    ) -> Result<DecimalColumn> {
        match self {
            DecimalDataType::Decimal128(size) => {
                let multiplier = i128::e(size.scale as u32);
                let min_for_precision = 9_i128
                    .saturating_pow(1 + size.precision as u32)
                    .saturating_neg();
                let max_for_precision = 9_i128.saturating_pow(1 + size.precision as u32);

                let mut values = Vec::with_capacity(from.len());

                for x in from.iter() {
                    let x = x.as_();
                    let x = x
                        .checked_mul(multiplier)
                        .and_then(|v| {
                            if v > max_for_precision || v < min_for_precision {
                                None
                            } else {
                                Some(v)
                            }
                        })
                        .ok_or_else(|| {
                            ErrorCode::Overflow(format!(
                                "Decimal overflow when converting from {:?} to {:?}",
                                x, size
                            ))
                        })?;
                    values.push(x);
                }
                Ok(DecimalColumn::Decimal128(values.into(), *size))
            }
            DecimalDataType::Decimal256(_size) => todo!(),
        }
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
