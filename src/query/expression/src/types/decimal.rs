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
use serde::Deserialize;
use serde::Serialize;

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

impl DecimalDataType {
    pub fn from_size(_size: DecimalSize) -> Result<DecimalDataType> {
        // todo!("decimal")
        Err(ErrorCode::Unimplemented("decimal"))
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
