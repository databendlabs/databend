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
use std::iter::TrustedLen;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::BuilderMut;
use super::DataType;
use super::NumberColumn;
use super::NumberDataType;
use super::ValueType;
use super::F32;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::TableDataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorType;

impl AccessType for VectorType {
    type Scalar = VectorScalar;
    type ScalarRef<'a> = VectorScalarRef<'a>;
    type Column = VectorColumn;
    type Domain = ();
    type ColumnIterator<'a> = VectorIterator<'a>;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_owned()
    }

    fn to_scalar_ref<'a>(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref()
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        scalar
            .as_vector()
            .cloned()
            .ok_or_else(|| scalar_type_error::<Self>(scalar))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        col.as_vector()
            .cloned()
            .ok_or_else(|| column_type_error::<Self>(col))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        if domain.is_undefined() {
            Ok(())
        } else {
            Err(domain_type_error::<Self>(domain))
        }
    }

    fn column_len<'a>(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.memory_size()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.partial_cmp(&rhs).unwrap_or(Ordering::Equal)
    }
}

impl ValueType for VectorType {
    type ColumnBuilder = VectorColumnBuilder;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_vector());
        Scalar::Vector(scalar)
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_vector());
        Column::Vector(col)
    }

    fn upcast_domain_with_type(_domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_vector());
        Domain::Undefined
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.as_vector_mut().unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_vector());
        Some(ColumnBuilder::Vector(builder))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        VectorColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(&item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        for _ in 0..n {
            builder.push(&item);
        }
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push_default();
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

#[macro_export]
macro_rules! with_vector_number_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Int8, Float32],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_vector_number_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Int8 => i8,
                Float32 => $super::number::F32,
            ],
            $($tail)*
        }
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
pub enum VectorDataType {
    Int8(u64),
    Float32(u64),
}

impl VectorDataType {
    pub fn dimension(&self) -> u64 {
        match self {
            VectorDataType::Int8(d) => *d,
            VectorDataType::Float32(d) => *d,
        }
    }

    pub fn default_value(&self) -> VectorScalar {
        match self {
            VectorDataType::Int8(d) => VectorScalar::Int8(vec![0_i8; *d as usize]),
            VectorDataType::Float32(d) => VectorScalar::Float32(vec![F32::from(0.0); *d as usize]),
        }
    }

    pub fn inner_data_type(&self) -> TableDataType {
        match self {
            VectorDataType::Int8(_) => TableDataType::Number(NumberDataType::Int8),
            VectorDataType::Float32(_) => TableDataType::Number(NumberDataType::Float32),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    EnumAsInner,
)]
pub enum VectorScalar {
    Int8(Vec<i8>),
    Float32(Vec<F32>),
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner, Hash)]
pub enum VectorScalarRef<'a> {
    Int8(&'a [i8]),
    Float32(&'a [F32]),
}

impl VectorScalar {
    pub fn as_ref(&self) -> VectorScalarRef<'_> {
        match self {
            VectorScalar::Int8(vals) => VectorScalarRef::Int8(vals),
            VectorScalar::Float32(vals) => VectorScalarRef::Float32(vals),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            VectorScalar::Int8(vals) => vals.len(),
            VectorScalar::Float32(vals) => vals.len(),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            VectorScalar::Int8(vals) => vals.len(),
            VectorScalar::Float32(vals) => vals.len() * 4,
        }
    }

    pub const fn dimension(&self) -> usize {
        match self {
            VectorScalar::Int8(vals) => vals.len(),
            VectorScalar::Float32(vals) => vals.len(),
        }
    }
}

impl<'a> VectorScalarRef<'a> {
    pub fn to_owned(&self) -> VectorScalar {
        match self {
            VectorScalarRef::Int8(vals) => VectorScalar::Int8(vals.to_vec()),
            VectorScalarRef::Float32(vals) => VectorScalar::Float32(vals.to_vec()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            VectorScalarRef::Int8(vals) => vals.len(),
            VectorScalarRef::Float32(vals) => vals.len(),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            VectorScalarRef::Int8(vals) => vals.len(),
            VectorScalarRef::Float32(vals) => vals.len() * 4,
        }
    }

    pub const fn dimension(&self) -> usize {
        match self {
            VectorScalarRef::Int8(vals) => vals.len(),
            VectorScalarRef::Float32(vals) => vals.len(),
        }
    }

    pub fn data_type(&self) -> VectorDataType {
        match self {
            VectorScalarRef::Int8(vals) => VectorDataType::Int8(vals.len() as u64),
            VectorScalarRef::Float32(vals) => VectorDataType::Float32(vals.len() as u64),
        }
    }
}

impl PartialOrd for VectorScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        crate::with_vector_number_type!(|NUM_TYPE| match (self, other) {
            (VectorScalar::NUM_TYPE(lhs), VectorScalar::NUM_TYPE(rhs)) => {
                if lhs.len() == rhs.len() {
                    lhs.iter().partial_cmp(rhs.iter())
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

impl PartialOrd for VectorScalarRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        crate::with_vector_number_type!(|NUM_TYPE| match (self, other) {
            (VectorScalarRef::NUM_TYPE(lhs), VectorScalarRef::NUM_TYPE(rhs)) => {
                if lhs.len() == rhs.len() {
                    lhs.iter().partial_cmp(rhs.iter())
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, EnumAsInner)]
pub enum VectorColumn {
    Int8((Buffer<i8>, usize)),
    Float32((Buffer<F32>, usize)),
}

#[derive(Clone, PartialEq, EnumAsInner, Debug)]
pub enum VectorColumnVec {
    Int8((Vec<Buffer<i8>>, usize)),
    Float32((Vec<Buffer<F32>>, usize)),
}

impl VectorColumn {
    pub fn data_type(&self) -> VectorDataType {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumn::NUM_TYPE((_, dimension)) => VectorDataType::NUM_TYPE(*dimension as u64),
        })
    }

    pub fn len(&self) -> usize {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumn::NUM_TYPE((values, dimension)) => values.len() / dimension,
        })
    }

    pub fn index(&self, index: usize) -> Option<VectorScalarRef<'_>> {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumn::NUM_TYPE((values, dimension)) => {
                let offset = index * dimension;
                if offset < values.len() {
                    let val = &values.as_slice()[offset..offset + dimension];
                    Some(VectorScalarRef::NUM_TYPE(val))
                } else {
                    None
                }
            }
        })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> VectorScalarRef<'_> {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumn::NUM_TYPE((values, dimension)) => {
                let offset = index * dimension;
                let val = &values.as_slice()[offset..offset + dimension];
                VectorScalarRef::NUM_TYPE(val)
            }
        })
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumn::NUM_TYPE((values, dimension)) => {
                let offset = range.start * dimension;
                let length = (range.end - range.start) * dimension;
                let mut range_values = values.clone();
                unsafe { range_values.slice_unchecked(offset, length) };
                VectorColumn::NUM_TYPE((range_values, *dimension))
            }
        })
    }

    pub fn underlying_column(&self) -> Column {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumn::NUM_TYPE((values, _)) => {
                Column::Number(NumberColumn::NUM_TYPE(values.clone()))
            }
        })
    }

    pub fn iter(&self) -> VectorIterator {
        VectorIterator {
            column: self,
            index: 0,
        }
    }

    pub const fn dimension(&self) -> usize {
        match self {
            VectorColumn::Int8((_, dimension)) => *dimension,
            VectorColumn::Float32((_, dimension)) => *dimension,
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            VectorColumn::Int8((values, _)) => values.len(),
            VectorColumn::Float32((values, _)) => values.len() * 4,
        }
    }
}

impl PartialOrd for VectorColumn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        crate::with_vector_number_type!(|NUM_TYPE| match (self, other) {
            (
                VectorColumn::NUM_TYPE((lhs, lhs_dimension)),
                VectorColumn::NUM_TYPE((rhs, rhs_dimension)),
            ) => {
                if lhs_dimension == rhs_dimension {
                    lhs.iter().partial_cmp(rhs.iter())
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum VectorColumnBuilder {
    Int8((Vec<i8>, usize)),
    Float32((Vec<F32>, usize)),
}

impl VectorColumnBuilder {
    pub fn with_capacity(ty: &VectorDataType, capacity: usize) -> Self {
        crate::with_vector_number_type!(|NUM_TYPE| match ty {
            VectorDataType::NUM_TYPE(dimension) =>
                VectorColumnBuilder::NUM_TYPE((Vec::with_capacity(capacity), *dimension as usize)),
        })
    }

    pub fn data_type(&self) -> VectorDataType {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumnBuilder::NUM_TYPE((_, dimension)) =>
                VectorDataType::NUM_TYPE(*dimension as u64),
        })
    }

    pub fn from_column(col: VectorColumn) -> Self {
        crate::with_vector_number_type!(|NUM_TYPE| match col {
            VectorColumn::NUM_TYPE((values, dimension)) => {
                VectorColumnBuilder::NUM_TYPE((values.as_slice().to_vec(), dimension))
            }
        })
    }

    pub fn repeat<'a>(item: &VectorScalarRef<'a>, n: usize) -> Self {
        crate::with_vector_number_type!(|NUM_TYPE| match item {
            VectorScalarRef::NUM_TYPE(item) => {
                let dimension = item.len();
                let mut values = Vec::with_capacity(dimension * n);
                for _ in 0..n {
                    values.extend_from_slice(item);
                }
                VectorColumnBuilder::NUM_TYPE((values, dimension))
            }
        })
    }

    pub fn repeat_default(ty: &VectorDataType, n: usize) -> Self {
        match ty {
            VectorDataType::Int8(dimension) => {
                let len = *dimension as usize * n;
                let values = vec![0_i8; len];
                VectorColumnBuilder::Int8((values, *dimension as usize))
            }
            VectorDataType::Float32(dimension) => {
                let len = *dimension as usize * n;
                let values = vec![F32::from(0.0); len];
                VectorColumnBuilder::Float32((values, *dimension as usize))
            }
        }
    }

    pub fn len(&self) -> usize {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumnBuilder::NUM_TYPE((values, dimension)) => values.len() / dimension,
        })
    }

    pub fn push<'a>(&mut self, item: &VectorScalarRef<'a>) {
        match (self, item) {
            (VectorColumnBuilder::Int8((values, dimension)), VectorScalarRef::Int8(item)) => {
                assert!(*dimension == item.len());
                values.extend_from_slice(item);
            }
            (VectorColumnBuilder::Float32((values, dimension)), VectorScalarRef::Float32(item)) => {
                assert!(*dimension == item.len());
                values.extend_from_slice(item);
            }
            (_, _) => unreachable!(),
        }
    }

    pub fn push_repeat<'a>(&mut self, item: &VectorScalarRef<'a>, n: usize) {
        if n == 0 {
            return;
        }
        match (self, item) {
            (VectorColumnBuilder::Int8((values, dimension)), VectorScalarRef::Int8(item)) => {
                assert!(*dimension == item.len());
                for _ in 0..n {
                    values.extend_from_slice(item);
                }
            }
            (VectorColumnBuilder::Float32((values, dimension)), VectorScalarRef::Float32(item)) => {
                assert!(*dimension == item.len());
                for _ in 0..n {
                    values.extend_from_slice(item);
                }
            }
            (_, _) => unreachable!(),
        }
    }

    pub fn push_default(&mut self) {
        match self {
            VectorColumnBuilder::Int8((values, dimension)) => {
                let new_len = values.len() + *dimension;
                values.resize(new_len, 0_i8);
            }
            VectorColumnBuilder::Float32((values, dimension)) => {
                let new_len = values.len() + *dimension;
                values.resize(new_len, F32::from(0.0));
            }
        }
    }

    pub fn append_column(&mut self, other: &VectorColumn) {
        match (self, other) {
            (
                VectorColumnBuilder::Int8((values, dimension)),
                VectorColumn::Int8((other_values, other_dimension)),
            ) => {
                assert!(*dimension == *other_dimension);
                values.extend_from_slice(other_values.as_slice());
            }
            (
                VectorColumnBuilder::Float32((values, dimension)),
                VectorColumn::Float32((other_values, other_dimension)),
            ) => {
                assert!(*dimension == *other_dimension);
                values.extend_from_slice(other_values.as_slice());
            }
            (_, _) => unreachable!(),
        }
    }

    pub fn build(self) -> VectorColumn {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumnBuilder::NUM_TYPE((values, dimension)) => {
                VectorColumn::NUM_TYPE((Buffer::from(values), dimension))
            }
        })
    }

    pub fn build_scalar(self) -> VectorScalar {
        crate::with_vector_number_type!(|NUM_TYPE| match self {
            VectorColumnBuilder::NUM_TYPE((values, dimension)) => {
                assert_eq!(values.len(), dimension);
                VectorScalar::NUM_TYPE(values.as_slice().to_vec())
            }
        })
    }

    pub fn pop(&mut self) -> Option<VectorScalar> {
        if self.len() > 0 {
            match self {
                VectorColumnBuilder::Int8((values, dimension)) => {
                    let offset = values.len() - *dimension;
                    Some(VectorScalar::Int8(values.split_off(offset)))
                }
                VectorColumnBuilder::Float32((values, dimension)) => {
                    let offset = values.len() - *dimension;
                    Some(VectorScalar::Float32(values.split_off(offset)))
                }
            }
        } else {
            None
        }
    }

    pub const fn dimension(&self) -> usize {
        match self {
            VectorColumnBuilder::Int8((_, dimension)) => *dimension,
            VectorColumnBuilder::Float32((_, dimension)) => *dimension,
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            VectorColumnBuilder::Int8((values, _)) => values.len(),
            VectorColumnBuilder::Float32((values, _)) => values.len() * 4,
        }
    }
}

pub struct VectorIterator<'a> {
    column: &'a VectorColumn,
    index: usize,
}

impl<'a> Iterator for VectorIterator<'a> {
    type Item = VectorScalarRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.column.len() {
            let value = unsafe { self.column.index_unchecked(self.index) };
            self.index += 1;
            Some(value)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.column.len();
        let size = len - self.index;
        (size, Some(size))
    }
}

unsafe impl TrustedLen for VectorIterator<'_> {}
