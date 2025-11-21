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

use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::BuilderMut;
use super::DataType;
use super::ValueType;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;

#[macro_export]
macro_rules! with_opaque_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Opaque1, Opaque2, Opaque3, Opaque4, Opaque5, Opaque6],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_opaque_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Opaque1 => 1,
                Opaque2 => 2,
                Opaque3 => 3,
                Opaque4 => 4,
                Opaque5 => 5,
                Opaque6 => 6
            ],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_opaque_size {
    (| $t:tt | $($tail:tt)*) => {{
        const N1: usize = 1;
        const N2: usize = 2;
        const N3: usize = 3;
        const N4: usize = 4;
        const N5: usize = 5;
        const N6: usize = 6;
        match_template::match_template! {
            $t = [N1, N2, N3, N4, N5, N6],
            $($tail)*
        }
    }}
}

#[macro_export]
macro_rules! with_opaque_size_mapped {
    (| $t:tt | $($tail:tt)*) => {{
        const N1: usize = 1;
        const N2: usize = 2;
        const N3: usize = 3;
        const N4: usize = 4;
        const N5: usize = 5;
        const N6: usize = 6;
        match_template::match_template! {
            $t = [N1 => Opaque1, N2 => Opaque2, N3 => Opaque3, N4 => Opaque4, N5 => Opaque5, N6 => Opaque6],
            $($tail)*
        }
    }}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueType<const N: usize>;

impl<const N: usize> AccessType for OpaqueType<N> {
    type Scalar = [u64; N];
    type ScalarRef<'a> = &'a [u64; N];
    type Column = Buffer<[u64; N]>;
    type Domain = ();
    type ColumnIterator<'a> = std::slice::Iter<'a, [u64; N]>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        *scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        let opaque = scalar
            .as_opaque()
            .ok_or_else(|| scalar_type_error::<Self>(scalar))?;
        match (opaque, N) {
            (OpaqueScalarRef::Opaque1(v), 1) => Ok(reinterpret_ref(v)),
            (OpaqueScalarRef::Opaque2(v), 2) => Ok(reinterpret_ref(v)),
            (OpaqueScalarRef::Opaque3(v), 3) => Ok(reinterpret_ref(v)),
            (OpaqueScalarRef::Opaque4(v), 4) => Ok(reinterpret_ref(v)),
            (OpaqueScalarRef::Opaque5(v), 5) => Ok(reinterpret_ref(v)),
            (OpaqueScalarRef::Opaque6(v), 6) => Ok(reinterpret_ref(v)),
            _ => Err(scalar_type_error::<Self>(scalar)),
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        if domain.is_undefined() {
            Ok(())
        } else {
            Err(domain_type_error::<Self>(domain))
        }
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let opaque_col = col
            .as_opaque()
            .ok_or_else(|| column_type_error::<Self>(col))?;
        unsafe {
            match (opaque_col, N) {
                (OpaqueColumn::Opaque1(buffer), 1) => Ok(std::mem::transmute(buffer.clone())),
                (OpaqueColumn::Opaque2(buffer), 2) => Ok(std::mem::transmute(buffer.clone())),
                (OpaqueColumn::Opaque3(buffer), 3) => Ok(std::mem::transmute(buffer.clone())),
                (OpaqueColumn::Opaque4(buffer), 4) => Ok(std::mem::transmute(buffer.clone())),
                (OpaqueColumn::Opaque5(buffer), 5) => Ok(std::mem::transmute(buffer.clone())),
                (OpaqueColumn::Opaque6(buffer), 6) => Ok(std::mem::transmute(buffer.clone())),
                _ => Err(column_type_error::<Self>(col)),
            }
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.get_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: std::ops::Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.len())
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.as_slice().iter()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> std::cmp::Ordering {
        lhs.cmp(rhs)
    }
}

impl<const N: usize> ValueType for OpaqueType<N> {
    type ColumnBuilder = Vec<[u64; N]>;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert_eq!(data_type.as_opaque(), Some(&N));
        with_opaque_size_mapped!(|T| match N {
            T => Scalar::Opaque(OpaqueScalar::T(reinterpret(scalar))),
            _ => unreachable!(),
        })
    }

    fn upcast_domain_with_type(_domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert_eq!(data_type.as_opaque(), Some(&N));
        Domain::Undefined
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert_eq!(data_type.as_opaque(), Some(&N));
        unsafe {
            with_opaque_size_mapped!(|T| match N {
                T => Column::Opaque(OpaqueColumn::T(std::mem::transmute(col))),
                _ => unreachable!(),
            })
        }
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        use std::mem::transmute;

        use OpaqueColumnBuilder::*;
        unsafe {
            match (builder.as_opaque_mut().unwrap(), N) {
                (Opaque1(b), 1) => transmute::<_, &mut Vec<[u64; N]>>(b).into(),
                (Opaque2(b), 2) => transmute::<_, &mut Vec<[u64; N]>>(b).into(),
                (Opaque3(b), 3) => transmute::<_, &mut Vec<[u64; N]>>(b).into(),
                (Opaque4(b), 4) => transmute::<_, &mut Vec<[u64; N]>>(b).into(),
                (Opaque5(b), 5) => transmute::<_, &mut Vec<[u64; N]>>(b).into(),
                (Opaque6(b), 6) => transmute::<_, &mut Vec<[u64; N]>>(b).into(),
                _ => panic!("type not match"),
            }
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert_eq!(data_type.as_opaque(), Some(&N));
        unsafe {
            with_opaque_size_mapped!(|T| match N {
                T => Some(ColumnBuilder::Opaque(OpaqueColumnBuilder::T(
                    std::mem::transmute(builder),
                ))),
                _ => unreachable!(),
            })
        }
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        col.to_vec()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(*item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        for _ in 0..n {
            builder.push(*item);
        }
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push([0; N]);
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.extend_from_slice(other.as_slice());
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        Buffer::from(builder)
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }
}

fn reinterpret<const S: usize, const D: usize>(v: [u64; S]) -> [u64; D] {
    if S == D {
        let ptr = v.as_ptr() as *const _;
        unsafe { *ptr }
    } else {
        unreachable!();
    }
}

fn reinterpret_ref<const S: usize, const D: usize>(v: &[u64; S]) -> &[u64; D] {
    if S == D {
        let ptr = v.as_ptr() as *const _;
        unsafe { &*ptr }
    } else {
        unreachable!();
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
pub enum OpaqueScalar {
    Opaque1([u64; 1]),
    Opaque2([u64; 2]),
    Opaque3([u64; 3]),
    Opaque4([u64; 4]),
    Opaque5([u64; 5]),
    Opaque6([u64; 6]),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OpaqueScalarRef<'a> {
    Opaque1(&'a [u64; 1]),
    Opaque2(&'a [u64; 2]),
    Opaque3(&'a [u64; 3]),
    Opaque4(&'a [u64; 4]),
    Opaque5(&'a [u64; 5]),
    Opaque6(&'a [u64; 6]),
}

impl OpaqueScalar {
    pub fn as_ref(&self) -> OpaqueScalarRef<'_> {
        with_opaque_type!(|T| match self {
            OpaqueScalar::T(arr) => OpaqueScalarRef::T(arr),
        })
    }

    pub fn size(&self) -> u8 {
        with_opaque_mapped_type!(|T| match self {
            OpaqueScalar::T(_) => T,
        })
    }

    pub fn data_type(&self) -> DataType {
        DataType::Opaque(self.size() as _)
    }
}

impl<'a> OpaqueScalarRef<'a> {
    pub fn to_owned(&self) -> OpaqueScalar {
        with_opaque_type!(|T| match self {
            OpaqueScalarRef::T(arr) => OpaqueScalar::T(**arr),
        })
    }

    pub fn size(&self) -> usize {
        with_opaque_mapped_type!(|T| match self {
            OpaqueScalarRef::T(_) => T,
        })
    }

    pub fn data_type(&self) -> DataType {
        DataType::Opaque(self.size())
    }

    pub fn as_slice(&self) -> &[u64] {
        with_opaque_type!(|T| match self {
            OpaqueScalarRef::T(arr) => arr.as_slice(),
        })
    }

    pub fn memory_size(&self) -> usize {
        self.size() * 8
    }

    pub fn to_le_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0_u8; self.memory_size()];
        for (i, v) in self.as_slice().iter().enumerate() {
            buffer.as_mut_slice()[i * 8..(i + 1) * 8].copy_from_slice(&v.to_le_bytes())
        }
        buffer
    }
}

impl OpaqueColumn {
    pub fn len(&self) -> usize {
        with_opaque_type!(|T| match self {
            OpaqueColumn::T(buffer) => buffer.len(),
        })
    }

    pub fn index(&self, index: usize) -> Option<OpaqueScalarRef<'_>> {
        with_opaque_type!(|T| match self {
            OpaqueColumn::T(buffer) => buffer.get(index).map(OpaqueScalarRef::T),
        })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> OpaqueScalarRef<'_> {
        with_opaque_type!(|T| match self {
            OpaqueColumn::T(buffer) => OpaqueScalarRef::T(buffer.get_unchecked(index)),
        })
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> OpaqueColumn {
        with_opaque_type!(|T| match self {
            OpaqueColumn::T(buffer) => {
                OpaqueColumn::T(buffer.clone().sliced(range.start, range.len()))
            }
        })
    }

    pub fn data_type(&self) -> DataType {
        DataType::Opaque(self.size())
    }

    pub fn memory_size(&self) -> usize {
        self.len() * 8 * self.size()
    }

    pub fn size(&self) -> usize {
        with_opaque_mapped_type!(|T| match self {
            OpaqueColumn::T(_) => T,
        })
    }

    pub fn arrow_data(&self) -> ArrayData {
        with_opaque_mapped_type!(|T| match self {
            OpaqueColumn::T(buffer) => {
                unsafe {
                    let child = ArrayDataBuilder::new(ArrowDataType::UInt64)
                        .len(self.len() * self.size())
                        .add_buffer(buffer.clone().into())
                        .build_unchecked();

                    ArrayDataBuilder::new(ArrowDataType::FixedSizeList(
                        Field::new_list_field(ArrowDataType::UInt64, false).into(),
                        T,
                    ))
                    .len(self.len())
                    .add_child_data(child)
                    .build_unchecked()
                }
            }
        })
    }

    pub fn try_from_arrow_data(data: ArrayData, size: usize) -> Result<Self> {
        with_opaque_size_mapped!(|T| match size {
            T => {
                let buffer = data.buffers()[0].clone().into();
                Ok(OpaqueColumn::T(buffer))
            }
            _ => Err(ErrorCode::Internal(format!(
                "Unsupported Opaque size: {size}. Only sizes 1-6 are supported."
            ))),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OpaqueColumn {
    Opaque1(Buffer<[u64; 1]>),
    Opaque2(Buffer<[u64; 2]>),
    Opaque3(Buffer<[u64; 3]>),
    Opaque4(Buffer<[u64; 4]>),
    Opaque5(Buffer<[u64; 5]>),
    Opaque6(Buffer<[u64; 6]>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OpaqueColumnVec {
    Opaque1(Vec<Buffer<[u64; 1]>>),
    Opaque2(Vec<Buffer<[u64; 2]>>),
    Opaque3(Vec<Buffer<[u64; 3]>>),
    Opaque4(Vec<Buffer<[u64; 4]>>),
    Opaque5(Vec<Buffer<[u64; 5]>>),
    Opaque6(Vec<Buffer<[u64; 6]>>),
}

#[derive(Debug, Clone)]
pub enum OpaqueColumnBuilder {
    Opaque1(Vec<[u64; 1]>),
    Opaque2(Vec<[u64; 2]>),
    Opaque3(Vec<[u64; 3]>),
    Opaque4(Vec<[u64; 4]>),
    Opaque5(Vec<[u64; 5]>),
    Opaque6(Vec<[u64; 6]>),
}

impl OpaqueColumnBuilder {
    pub fn len(&self) -> usize {
        with_opaque_type!(|T| match self {
            OpaqueColumnBuilder::T(builder) => builder.len(),
        })
    }

    pub fn memory_size(&self) -> usize {
        with_opaque_mapped_type!(|T| match self {
            OpaqueColumnBuilder::T(builder) => builder.len() * (T as usize * 8),
        })
    }

    pub fn data_type(&self) -> DataType {
        with_opaque_mapped_type!(|T| match self {
            OpaqueColumnBuilder::T(_) => DataType::Opaque(T),
        })
    }

    pub fn push_default(&mut self) {
        with_opaque_type!(|T| match self {
            OpaqueColumnBuilder::T(builder) => builder.push(unsafe { std::mem::zeroed() }),
        })
    }

    pub fn pop(&mut self) -> Option<OpaqueScalar> {
        with_opaque_type!(|T| match self {
            OpaqueColumnBuilder::T(builder) => builder.pop().map(OpaqueScalar::T),
        })
    }

    pub fn build(self) -> OpaqueColumn {
        with_opaque_type!(|T| match self {
            OpaqueColumnBuilder::T(builder) => OpaqueColumn::T(Buffer::from(builder)),
        })
    }

    pub fn build_scalar(self) -> OpaqueScalar {
        with_opaque_type!(|T| match self {
            OpaqueColumnBuilder::T(builder) => {
                assert_eq!(builder.len(), 1);
                OpaqueScalar::T(builder[0])
            }
        })
    }

    pub fn from_column(col: OpaqueColumn) -> Self {
        with_opaque_type!(|T| match col {
            OpaqueColumn::T(buffer) => OpaqueColumnBuilder::T(buffer.to_vec()),
        })
    }
}

impl<const N: usize> super::ReturnType for OpaqueType<N> {
    fn create_builder(capacity: usize, _: &super::GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }
}

impl<const N: usize> super::ArgType for OpaqueType<N> {
    fn data_type() -> DataType {
        DataType::Opaque(N)
    }

    fn full_domain() -> Self::Domain {}
}
