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
use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use num_traits::NumCast;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub type F32 = OrderedFloat<f32>;
pub type F64 = OrderedFloat<f64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberType<T: Number>(PhantomData<T>);

pub type Int8Type = NumberType<i8>;
pub type Int16Type = NumberType<i16>;
pub type Int32Type = NumberType<i32>;
pub type Int64Type = NumberType<i64>;
pub type UInt8Type = NumberType<u8>;
pub type UInt16Type = NumberType<u16>;
pub type UInt32Type = NumberType<u32>;
pub type UInt64Type = NumberType<u64>;
pub type Float32Type = NumberType<F32>;
pub type Float64Type = NumberType<F64>;

impl<Num: Number> ValueType for NumberType<Num> {
    type Scalar = Num;
    type ScalarRef<'a> = Num;
    type Column = Buffer<Num>;
    type Domain = SimpleDomain<Num>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, Num>>;
    type ColumnBuilder = Vec<Num>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Num) -> Num {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        Num::try_downcast_scalar(scalar.as_number()?)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        Num::try_downcast_column(col.as_number()?)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<SimpleDomain<Num>> {
        Num::try_downcast_domain(domain.as_number()?)
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Number(num) => Num::try_downcast_builder(num),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Number(Num::upcast_scalar(scalar))
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Number(Num::upcast_column(col))
    }

    fn upcast_domain(domain: SimpleDomain<Num>) -> Domain {
        Domain::Number(Num::upcast_domain(domain))
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
        col.clone().slice(range.start, range.end - range.start)
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

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::Scalar) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Num::default());
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other: &Self::ColumnBuilder) {
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

impl<Num: Number> ArgType for NumberType<Num> {
    fn data_type() -> DataType {
        DataType::Number(Num::data_type())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumAsInner)]
pub enum NumberDataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

#[derive(Clone, Copy, PartialEq, Eq, EnumAsInner, Serialize, Deserialize)]
pub enum NumberScalar {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(F32),
    Float64(F64),
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum NumberColumn {
    UInt8(Buffer<u8>),
    UInt16(Buffer<u16>),
    UInt32(Buffer<u32>),
    UInt64(Buffer<u64>),
    Int8(Buffer<i8>),
    Int16(Buffer<i16>),
    Int32(Buffer<i32>),
    Int64(Buffer<i64>),
    Float32(Buffer<F32>),
    Float64(Buffer<F64>),
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum NumberColumnBuilder {
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<F32>),
    Float64(Vec<F64>),
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum NumberDomain {
    UInt8(SimpleDomain<u8>),
    UInt16(SimpleDomain<u16>),
    UInt32(SimpleDomain<u32>),
    UInt64(SimpleDomain<u64>),
    Int8(SimpleDomain<i8>),
    Int16(SimpleDomain<i16>),
    Int32(SimpleDomain<i32>),
    Int64(SimpleDomain<i64>),
    Float32(SimpleDomain<F32>),
    Float64(SimpleDomain<F64>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleDomain<T> {
    pub min: T,
    pub max: T,
}

impl NumberDataType {
    pub const fn new(bit_width: u8, is_signed: bool, is_float: bool) -> Self {
        match (bit_width, is_signed, is_float) {
            (8, false, false) => NumberDataType::UInt8,
            (16, false, false) => NumberDataType::UInt16,
            (32, false, false) => NumberDataType::UInt32,
            (64, false, false) => NumberDataType::UInt64,
            (8, true, false) => NumberDataType::Int8,
            (16, true, false) => NumberDataType::Int16,
            (32, true, false) => NumberDataType::Int32,
            (64, true, false) => NumberDataType::Int64,
            (32, true, true) => NumberDataType::Float32,
            (64, true, true) => NumberDataType::Float64,
            _ => panic!("unsupported numeric type"),
        }
    }

    pub const fn bit_width(&self) -> u8 {
        match self {
            NumberDataType::UInt8 => 8,
            NumberDataType::UInt16 => 16,
            NumberDataType::UInt32 => 32,
            NumberDataType::UInt64 => 64,
            NumberDataType::Int8 => 8,
            NumberDataType::Int16 => 16,
            NumberDataType::Int32 => 32,
            NumberDataType::Int64 => 64,
            NumberDataType::Float32 => 32,
            NumberDataType::Float64 => 64,
        }
    }

    pub const fn is_signed(&self) -> bool {
        match self {
            NumberDataType::UInt8 => false,
            NumberDataType::UInt16 => false,
            NumberDataType::UInt32 => false,
            NumberDataType::UInt64 => false,
            NumberDataType::Int8 => true,
            NumberDataType::Int16 => true,
            NumberDataType::Int32 => true,
            NumberDataType::Int64 => true,
            NumberDataType::Float32 => true,
            NumberDataType::Float64 => true,
        }
    }

    pub const fn is_float(&self) -> bool {
        match self {
            NumberDataType::UInt8 => false,
            NumberDataType::UInt16 => false,
            NumberDataType::UInt32 => false,
            NumberDataType::UInt64 => false,
            NumberDataType::Int8 => false,
            NumberDataType::Int16 => false,
            NumberDataType::Int32 => false,
            NumberDataType::Int64 => false,
            NumberDataType::Float32 => true,
            NumberDataType::Float64 => true,
        }
    }

    pub const fn can_lossless_cast_to(self, dest: Self) -> bool {
        match (self.is_float(), dest.is_float()) {
            (true, true) => self.bit_width() <= dest.bit_width(),
            (true, false) => false,
            (false, true) => self.bit_width() < dest.bit_width(),
            (false, false) => match (self.is_signed(), dest.is_signed()) {
                (true, true) | (false, false) => self.bit_width() <= dest.bit_width(),
                (false, true) => {
                    if let Some(self_next_bit_width) = next_bit_width(self.bit_width()) {
                        self_next_bit_width <= dest.bit_width()
                    } else {
                        false
                    }
                }
                (true, false) => false,
            },
        }
    }

    pub const fn lossless_super_type(self, other: Self) -> Option<Self> {
        Some(match (self.is_float(), other.is_float()) {
            (true, true) => NumberDataType::new(
                max_bit_with(self.bit_width(), other.bit_width()),
                true,
                true,
            ),
            (true, false) => {
                let bin_width =
                    if let Some(next_other_bit_width) = next_bit_width(other.bit_width()) {
                        max_bit_with(self.bit_width(), next_other_bit_width)
                    } else {
                        return None;
                    };
                NumberDataType::new(bin_width, true, true)
            }
            (false, true) => {
                let bin_width = if let Some(next_self_bit_width) = next_bit_width(self.bit_width())
                {
                    max_bit_with(next_self_bit_width, other.bit_width())
                } else {
                    return None;
                };
                NumberDataType::new(bin_width, true, true)
            }
            (false, false) => match (self.is_signed(), other.is_signed()) {
                (true, true) => NumberDataType::new(
                    max_bit_with(self.bit_width(), other.bit_width()),
                    true,
                    false,
                ),
                (false, false) => NumberDataType::new(
                    max_bit_with(self.bit_width(), other.bit_width()),
                    false,
                    false,
                ),
                (false, true) => {
                    let bin_width =
                        if let Some(next_other_bit_width) = next_bit_width(other.bit_width()) {
                            max_bit_with(self.bit_width(), next_other_bit_width)
                        } else {
                            return None;
                        };
                    NumberDataType::new(bin_width, true, false)
                }
                (true, false) => {
                    let bin_width =
                        if let Some(next_self_bit_width) = next_bit_width(self.bit_width()) {
                            max_bit_with(next_self_bit_width, other.bit_width())
                        } else {
                            return None;
                        };
                    NumberDataType::new(bin_width, true, false)
                }
            },
        })
    }
}

const fn next_bit_width(width: u8) -> Option<u8> {
    match width {
        8 => Some(16),
        16 => Some(32),
        32 => Some(64),
        64 => None,
        _ => panic!("invalid bit width"),
    }
}

const fn max_bit_with(lhs: u8, rhs: u8) -> u8 {
    if lhs > rhs { lhs } else { rhs }
}

impl PartialOrd for NumberScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (NumberScalar::UInt8(lhs), NumberScalar::UInt8(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::UInt16(lhs), NumberScalar::UInt16(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::UInt32(lhs), NumberScalar::UInt32(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::UInt64(lhs), NumberScalar::UInt64(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::Int8(lhs), NumberScalar::Int8(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::Int16(lhs), NumberScalar::Int16(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::Int32(lhs), NumberScalar::Int32(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::Int64(lhs), NumberScalar::Int64(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::Float32(lhs), NumberScalar::Float32(rhs)) => lhs.partial_cmp(rhs),
            (NumberScalar::Float64(lhs), NumberScalar::Float64(rhs)) => lhs.partial_cmp(rhs),
            _ => None,
        }
    }
}

impl NumberScalar {
    pub fn repeat(&self, n: usize) -> NumberColumnBuilder {
        match self {
            NumberScalar::UInt8(num) => NumberColumnBuilder::UInt8(vec![*num; n]),
            NumberScalar::UInt16(num) => NumberColumnBuilder::UInt16(vec![*num; n]),
            NumberScalar::UInt32(num) => NumberColumnBuilder::UInt32(vec![*num; n]),
            NumberScalar::UInt64(num) => NumberColumnBuilder::UInt64(vec![*num; n]),
            NumberScalar::Int8(num) => NumberColumnBuilder::Int8(vec![*num; n]),
            NumberScalar::Int16(num) => NumberColumnBuilder::Int16(vec![*num; n]),
            NumberScalar::Int32(num) => NumberColumnBuilder::Int32(vec![*num; n]),
            NumberScalar::Int64(num) => NumberColumnBuilder::Int64(vec![*num; n]),
            NumberScalar::Float32(num) => NumberColumnBuilder::Float32(vec![*num; n]),
            NumberScalar::Float64(num) => NumberColumnBuilder::Float64(vec![*num; n]),
        }
    }

    pub fn domain(&self) -> NumberDomain {
        match self {
            NumberScalar::UInt8(num) => NumberDomain::UInt8(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::UInt16(num) => NumberDomain::UInt16(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::UInt32(num) => NumberDomain::UInt32(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::UInt64(num) => NumberDomain::UInt64(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::Int8(num) => NumberDomain::Int8(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::Int16(num) => NumberDomain::Int16(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::Int32(num) => NumberDomain::Int32(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::Int64(num) => NumberDomain::Int64(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::Float32(num) => NumberDomain::Float32(SimpleDomain {
                min: *num,
                max: *num,
            }),
            NumberScalar::Float64(num) => NumberDomain::Float64(SimpleDomain {
                min: *num,
                max: *num,
            }),
        }
    }
}

impl NumberColumn {
    pub fn len(&self) -> usize {
        match self {
            NumberColumn::UInt8(col) => col.len(),
            NumberColumn::UInt16(col) => col.len(),
            NumberColumn::UInt32(col) => col.len(),
            NumberColumn::UInt64(col) => col.len(),
            NumberColumn::Int8(col) => col.len(),
            NumberColumn::Int16(col) => col.len(),
            NumberColumn::Int32(col) => col.len(),
            NumberColumn::Int64(col) => col.len(),
            NumberColumn::Float32(col) => col.len(),
            NumberColumn::Float64(col) => col.len(),
        }
    }

    pub fn index(&self, index: usize) -> Option<NumberScalar> {
        match self {
            NumberColumn::UInt8(col) => Some(NumberScalar::UInt8(col.get(index).cloned()?)),
            NumberColumn::UInt16(col) => Some(NumberScalar::UInt16(col.get(index).cloned()?)),
            NumberColumn::UInt32(col) => Some(NumberScalar::UInt32(col.get(index).cloned()?)),
            NumberColumn::UInt64(col) => Some(NumberScalar::UInt64(col.get(index).cloned()?)),
            NumberColumn::Int8(col) => Some(NumberScalar::Int8(col.get(index).cloned()?)),
            NumberColumn::Int16(col) => Some(NumberScalar::Int16(col.get(index).cloned()?)),
            NumberColumn::Int32(col) => Some(NumberScalar::Int32(col.get(index).cloned()?)),
            NumberColumn::Int64(col) => Some(NumberScalar::Int64(col.get(index).cloned()?)),
            NumberColumn::Float32(col) => Some(NumberScalar::Float32(col.get(index).cloned()?)),
            NumberColumn::Float64(col) => Some(NumberScalar::Float64(col.get(index).cloned()?)),
        }
    }

    /// # Safety
    /// Assumes that the `index` is not out of range.
    pub unsafe fn index_unchecked(&self, index: usize) -> NumberScalar {
        match self {
            NumberColumn::UInt8(col) => NumberScalar::UInt8(*col.get_unchecked(index)),
            NumberColumn::UInt16(col) => NumberScalar::UInt16(*col.get_unchecked(index)),
            NumberColumn::UInt32(col) => NumberScalar::UInt32(*col.get_unchecked(index)),
            NumberColumn::UInt64(col) => NumberScalar::UInt64(*col.get_unchecked(index)),
            NumberColumn::Int8(col) => NumberScalar::Int8(*col.get_unchecked(index)),
            NumberColumn::Int16(col) => NumberScalar::Int16(*col.get_unchecked(index)),
            NumberColumn::Int32(col) => NumberScalar::Int32(*col.get_unchecked(index)),
            NumberColumn::Int64(col) => NumberScalar::Int64(*col.get_unchecked(index)),
            NumberColumn::Float32(col) => NumberScalar::Float32(*col.get_unchecked(index)),
            NumberColumn::Float64(col) => NumberScalar::Float64(*col.get_unchecked(index)),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );
        match self {
            NumberColumn::UInt8(col) => {
                NumberColumn::UInt8(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::UInt16(col) => {
                NumberColumn::UInt16(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::UInt32(col) => {
                NumberColumn::UInt32(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::UInt64(col) => {
                NumberColumn::UInt64(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::Int8(col) => {
                NumberColumn::Int8(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::Int16(col) => {
                NumberColumn::Int16(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::Int32(col) => {
                NumberColumn::Int32(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::Int64(col) => {
                NumberColumn::Int64(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::Float32(col) => {
                NumberColumn::Float32(col.clone().slice(range.start, range.end - range.start))
            }
            NumberColumn::Float64(col) => {
                NumberColumn::Float64(col.clone().slice(range.start, range.end - range.start))
            }
        }
    }

    pub fn domain(&self) -> NumberDomain {
        assert!(self.len() > 0);
        match self {
            NumberColumn::UInt8(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::UInt8(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::UInt16(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::UInt16(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::UInt32(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::UInt32(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::UInt64(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::UInt64(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::Int8(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::Int8(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::Int16(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::Int16(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::Int32(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::Int32(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::Int64(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::Int64(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::Float32(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::Float32(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            NumberColumn::Float64(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                NumberDomain::Float64(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
        }
    }
}

impl NumberColumnBuilder {
    pub fn from_column(col: NumberColumn) -> Self {
        match col {
            NumberColumn::UInt8(col) => NumberColumnBuilder::UInt8(buffer_into_mut(col)),
            NumberColumn::UInt16(col) => NumberColumnBuilder::UInt16(buffer_into_mut(col)),
            NumberColumn::UInt32(col) => NumberColumnBuilder::UInt32(buffer_into_mut(col)),
            NumberColumn::UInt64(col) => NumberColumnBuilder::UInt64(buffer_into_mut(col)),
            NumberColumn::Int8(col) => NumberColumnBuilder::Int8(buffer_into_mut(col)),
            NumberColumn::Int16(col) => NumberColumnBuilder::Int16(buffer_into_mut(col)),
            NumberColumn::Int32(col) => NumberColumnBuilder::Int32(buffer_into_mut(col)),
            NumberColumn::Int64(col) => NumberColumnBuilder::Int64(buffer_into_mut(col)),
            NumberColumn::Float32(col) => NumberColumnBuilder::Float32(buffer_into_mut(col)),
            NumberColumn::Float64(col) => NumberColumnBuilder::Float64(buffer_into_mut(col)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NumberColumnBuilder::UInt8(col) => col.len(),
            NumberColumnBuilder::UInt16(col) => col.len(),
            NumberColumnBuilder::UInt32(col) => col.len(),
            NumberColumnBuilder::UInt64(col) => col.len(),
            NumberColumnBuilder::Int8(col) => col.len(),
            NumberColumnBuilder::Int16(col) => col.len(),
            NumberColumnBuilder::Int32(col) => col.len(),
            NumberColumnBuilder::Int64(col) => col.len(),
            NumberColumnBuilder::Float32(col) => col.len(),
            NumberColumnBuilder::Float64(col) => col.len(),
        }
    }

    pub fn with_capacity(ty: &NumberDataType, capacity: usize) -> Self {
        match ty {
            NumberDataType::UInt8 => NumberColumnBuilder::UInt8(Vec::with_capacity(capacity)),
            NumberDataType::UInt16 => NumberColumnBuilder::UInt16(Vec::with_capacity(capacity)),
            NumberDataType::UInt32 => NumberColumnBuilder::UInt32(Vec::with_capacity(capacity)),
            NumberDataType::UInt64 => NumberColumnBuilder::UInt64(Vec::with_capacity(capacity)),
            NumberDataType::Int8 => NumberColumnBuilder::Int8(Vec::with_capacity(capacity)),
            NumberDataType::Int16 => NumberColumnBuilder::Int16(Vec::with_capacity(capacity)),
            NumberDataType::Int32 => NumberColumnBuilder::Int32(Vec::with_capacity(capacity)),
            NumberDataType::Int64 => NumberColumnBuilder::Int64(Vec::with_capacity(capacity)),
            NumberDataType::Float32 => NumberColumnBuilder::Float32(Vec::with_capacity(capacity)),
            NumberDataType::Float64 => NumberColumnBuilder::Float64(Vec::with_capacity(capacity)),
        }
    }

    pub fn push(&mut self, item: NumberScalar) {
        match (self, item) {
            (NumberColumnBuilder::UInt8(builder), NumberScalar::UInt8(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::UInt16(builder), NumberScalar::UInt16(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::UInt32(builder), NumberScalar::UInt32(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::UInt64(builder), NumberScalar::UInt64(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::Int8(builder), NumberScalar::Int8(value)) => builder.push(value),
            (NumberColumnBuilder::Int16(builder), NumberScalar::Int16(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::Int32(builder), NumberScalar::Int32(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::Int64(builder), NumberScalar::Int64(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::Float32(builder), NumberScalar::Float32(value)) => {
                builder.push(value)
            }
            (NumberColumnBuilder::Float64(builder), NumberScalar::Float64(value)) => {
                builder.push(value)
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        }
    }

    pub fn push_default(&mut self) {
        match self {
            NumberColumnBuilder::UInt8(builder) => builder.push(0),
            NumberColumnBuilder::UInt16(builder) => builder.push(0),
            NumberColumnBuilder::UInt32(builder) => builder.push(0),
            NumberColumnBuilder::UInt64(builder) => builder.push(0),
            NumberColumnBuilder::Int8(builder) => builder.push(0),
            NumberColumnBuilder::Int16(builder) => builder.push(0),
            NumberColumnBuilder::Int32(builder) => builder.push(0),
            NumberColumnBuilder::Int64(builder) => builder.push(0),
            NumberColumnBuilder::Float32(builder) => builder.push(0.0.into()),
            NumberColumnBuilder::Float64(builder) => builder.push(0.0.into()),
        }
    }

    pub fn append(&mut self, other: &NumberColumnBuilder) {
        match (self, other) {
            (NumberColumnBuilder::UInt8(builder), NumberColumnBuilder::UInt8(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::UInt16(builder), NumberColumnBuilder::UInt16(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::UInt32(builder), NumberColumnBuilder::UInt32(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::UInt64(builder), NumberColumnBuilder::UInt64(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::Int8(builder), NumberColumnBuilder::Int8(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::Int16(builder), NumberColumnBuilder::Int16(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::Int32(builder), NumberColumnBuilder::Int32(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (NumberColumnBuilder::Int64(builder), NumberColumnBuilder::Int64(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (
                NumberColumnBuilder::Float32(builder),
                NumberColumnBuilder::Float32(other_builder),
            ) => {
                builder.extend_from_slice(other_builder);
            }
            (
                NumberColumnBuilder::Float64(builder),
                NumberColumnBuilder::Float64(other_builder),
            ) => {
                builder.extend_from_slice(other_builder);
            }
            (this, other) => unreachable!("unable append {other:?} onto {this:?}"),
        }
    }

    pub fn build(self) -> NumberColumn {
        match self {
            NumberColumnBuilder::UInt8(builder) => NumberColumn::UInt8(builder.into()),
            NumberColumnBuilder::UInt16(builder) => NumberColumn::UInt16(builder.into()),
            NumberColumnBuilder::UInt32(builder) => NumberColumn::UInt32(builder.into()),
            NumberColumnBuilder::UInt64(builder) => NumberColumn::UInt64(builder.into()),
            NumberColumnBuilder::Int8(builder) => NumberColumn::Int8(builder.into()),
            NumberColumnBuilder::Int16(builder) => NumberColumn::Int16(builder.into()),
            NumberColumnBuilder::Int32(builder) => NumberColumn::Int32(builder.into()),
            NumberColumnBuilder::Int64(builder) => NumberColumn::Int64(builder.into()),
            NumberColumnBuilder::Float32(builder) => NumberColumn::Float32(builder.into()),
            NumberColumnBuilder::Float64(builder) => NumberColumn::Float64(builder.into()),
        }
    }

    pub fn build_scalar(self) -> NumberScalar {
        assert_eq!(self.len(), 1);
        match self {
            NumberColumnBuilder::UInt8(builder) => NumberScalar::UInt8(builder[0]),
            NumberColumnBuilder::UInt16(builder) => NumberScalar::UInt16(builder[0]),
            NumberColumnBuilder::UInt32(builder) => NumberScalar::UInt32(builder[0]),
            NumberColumnBuilder::UInt64(builder) => NumberScalar::UInt64(builder[0]),
            NumberColumnBuilder::Int8(builder) => NumberScalar::Int8(builder[0]),
            NumberColumnBuilder::Int16(builder) => NumberScalar::Int16(builder[0]),
            NumberColumnBuilder::Int32(builder) => NumberScalar::Int32(builder[0]),
            NumberColumnBuilder::Int64(builder) => NumberScalar::Int64(builder[0]),
            NumberColumnBuilder::Float32(builder) => NumberScalar::Float32(builder[0]),
            NumberColumnBuilder::Float64(builder) => NumberScalar::Float64(builder[0]),
        }
    }
}

impl<T: Number> SimpleDomain<T> {
    /// Returns the saturating cast domain and a flag denoting whether overflow happened.
    pub fn overflow_cast<U: Number>(&self) -> (SimpleDomain<U>, bool) {
        let (min, min_overflowing) = overflow_cast::<T, U>(self.min);
        let (max, max_overflowing) = overflow_cast::<T, U>(self.max);
        (
            SimpleDomain { min, max },
            min_overflowing || max_overflowing,
        )
    }
}

fn overflow_cast<T: Number, U: Number>(src: T) -> (U, bool) {
    let dest_min: T = num_traits::cast(U::MIN).unwrap_or(T::MIN);
    let dest_max: T = num_traits::cast(U::MAX).unwrap_or(T::MAX);
    let src_clamp: T = src.clamp(dest_min, dest_max);
    let overflowing = src != src_clamp;
    // The number must be within the range that `U` can represent after clamping, therefore
    // it's safe to unwrap.
    let dest: U = num_traits::cast(src_clamp).unwrap();

    (dest, overflowing)
}

#[macro_export]
macro_rules! with_number_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_number_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                UInt8 => u8, UInt16 => u16, UInt32 => u32, UInt64 => u64,
                Int8 => i8, Int16 => i16, Int32 => i32, Int64 => i64,
                Float32 => $crate::types::number::F32, Float64 => $crate::types::number::F64
            ],
            $($tail)*
        }
    }
}

pub trait Number:
    Copy
    + Debug
    + NumCast
    + Default
    + Clone
    + Copy
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + Sync
    + Send
    + 'static
{
    const MIN: Self;
    const MAX: Self;

    fn data_type() -> NumberDataType;
    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self>;
    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>>;
    // TODO
    fn try_downcast_builder(col: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>>;
    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>>;
    fn upcast_scalar(scalar: Self) -> NumberScalar;
    fn upcast_column(col: Buffer<Self>) -> NumberColumn;
    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain;
}

impl Number for u8 {
    const MIN: Self = u8::MIN;
    const MAX: Self = u8::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::UInt8
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_u_int8().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_u_int8().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_u_int8_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_u_int8().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::UInt8(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::UInt8(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::UInt8(domain)
    }
}

impl Number for u16 {
    const MIN: Self = u16::MIN;
    const MAX: Self = u16::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::UInt16
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_u_int16().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_u_int16().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_u_int16_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_u_int16().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::UInt16(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::UInt16(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::UInt16(domain)
    }
}

impl Number for u32 {
    const MIN: Self = u32::MIN;
    const MAX: Self = u32::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::UInt32
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_u_int32().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_u_int32().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_u_int32_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_u_int32().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::UInt32(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::UInt32(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::UInt32(domain)
    }
}

impl Number for u64 {
    const MIN: Self = u64::MIN;
    const MAX: Self = u64::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::UInt64
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_u_int64().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_u_int64().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_u_int64_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_u_int64().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::UInt64(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::UInt64(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::UInt64(domain)
    }
}

impl Number for i8 {
    const MIN: Self = i8::MIN;
    const MAX: Self = i8::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::Int8
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_int8().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_int8().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_int8_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_int8().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::Int8(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::Int8(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::Int8(domain)
    }
}

impl Number for i16 {
    const MIN: Self = i16::MIN;
    const MAX: Self = i16::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::Int16
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_int16().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_int16().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_int16_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_int16().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::Int16(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::Int16(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::Int16(domain)
    }
}

impl Number for i32 {
    const MIN: Self = i32::MIN;
    const MAX: Self = i32::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::Int32
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_int32().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_int32().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_int32_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_int32().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::Int32(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::Int32(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::Int32(domain)
    }
}

impl Number for i64 {
    const MIN: Self = i64::MIN;
    const MAX: Self = i64::MAX;

    fn data_type() -> NumberDataType {
        NumberDataType::Int64
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_int64().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_int64().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_int64_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_int64().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::Int64(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::Int64(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::Int64(domain)
    }
}

impl Number for F32 {
    const MIN: Self = OrderedFloat(f32::NEG_INFINITY);
    const MAX: Self = OrderedFloat(f32::NAN);

    fn data_type() -> NumberDataType {
        NumberDataType::Float32
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_float32().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_float32().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_float32_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_float32().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::Float32(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::Float32(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::Float32(domain)
    }
}

impl Number for F64 {
    const MIN: Self = OrderedFloat(f64::NEG_INFINITY);
    const MAX: Self = OrderedFloat(f64::NAN);

    fn data_type() -> NumberDataType {
        NumberDataType::Float64
    }

    fn try_downcast_scalar(scalar: &NumberScalar) -> Option<Self> {
        scalar.as_float64().cloned()
    }

    fn try_downcast_column(col: &NumberColumn) -> Option<Buffer<Self>> {
        col.as_float64().cloned()
    }

    fn try_downcast_builder(builder: &mut NumberColumnBuilder) -> Option<&mut Vec<Self>> {
        builder.as_float64_mut()
    }

    fn try_downcast_domain(domain: &NumberDomain) -> Option<SimpleDomain<Self>> {
        domain.as_float64().cloned()
    }

    fn upcast_scalar(scalar: Self) -> NumberScalar {
        NumberScalar::Float64(scalar)
    }

    fn upcast_column(col: Buffer<Self>) -> NumberColumn {
        NumberColumn::Float64(col)
    }

    fn upcast_domain(domain: SimpleDomain<Self>) -> NumberDomain {
        NumberDomain::Float64(domain)
    }
}
