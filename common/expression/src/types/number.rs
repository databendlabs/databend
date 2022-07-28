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
use common_arrow::arrow::types::NativeType;

use crate::property::Domain;
use crate::property::FloatDomain;
use crate::property::IntDomain;
use crate::property::UIntDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

pub trait Number: Debug + Clone + PartialEq + 'static {
    type Storage: NativeType;
    type Domain: Debug + Clone + PartialEq;

    fn data_type() -> DataType;
    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage>;
    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>>;
    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain>;
    fn upcast_scalar(scalar: Self::Storage) -> Scalar;
    fn upcast_column(col: Buffer<Self::Storage>) -> Column;
    fn upcast_domain(domain: Self::Domain) -> Domain;
    fn full_domain() -> Self::Domain;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberType<T: Number>(PhantomData<T>);

impl<Int: Number> ValueType for NumberType<Int> {
    type Scalar = Int::Storage;
    type ScalarRef<'a> = Int::Storage;
    type Column = Buffer<Int::Storage>;
    type Domain = Int::Domain;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, Int::Storage>>;
    type ColumnBuilder = Vec<Int::Storage>;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        Int::try_downcast_scalar(scalar)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        Int::try_downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        Int::try_downcast_domain(domain)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Int::upcast_scalar(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Int::upcast_column(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Int::upcast_domain(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index).cloned()
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
        builder.push(Int::Storage::default());
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.extend_from_slice(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }
}

impl<Int: Number> ArgType for NumberType<Int> {
    fn data_type() -> DataType {
        Int::data_type()
    }

    fn full_domain(_: &GenericMap) -> Self::Domain {
        Int::full_domain()
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }
}

impl Number for u8 {
    type Storage = u8;
    type Domain = UIntDomain;

    fn data_type() -> DataType {
        DataType::UInt8
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int8().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int8().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_u_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt8(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt8(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::UInt(domain)
    }

    fn full_domain() -> Self::Domain {
        UIntDomain {
            min: 0,
            max: u8::MAX as u64,
        }
    }
}

impl Number for u16 {
    type Storage = u16;
    type Domain = UIntDomain;

    fn data_type() -> DataType {
        DataType::UInt16
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int16().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int16().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_u_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt16(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt16(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::UInt(domain)
    }

    fn full_domain() -> Self::Domain {
        UIntDomain {
            min: 0,
            max: u16::MAX as u64,
        }
    }
}

impl Number for u32 {
    type Storage = u32;
    type Domain = UIntDomain;

    fn data_type() -> DataType {
        DataType::UInt32
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_u_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt32(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::UInt(domain)
    }

    fn full_domain() -> Self::Domain {
        UIntDomain {
            min: 0,
            max: u32::MAX as u64,
        }
    }
}

impl Number for u64 {
    type Storage = u64;
    type Domain = UIntDomain;

    fn data_type() -> DataType {
        DataType::UInt64
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_u_int().cloned()
    }
    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt64(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::UInt(domain)
    }
    fn full_domain() -> Self::Domain {
        UIntDomain {
            min: 0,
            max: u64::MAX,
        }
    }
}

impl Number for i8 {
    type Storage = i8;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Int8
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int8().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int8().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int8(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int8(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i8::MIN as i64,
            max: i8::MAX as i64,
        }
    }
}

impl Number for i16 {
    type Storage = i16;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Int16
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int16().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int16().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int16(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int16(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i16::MIN as i64,
            max: i16::MAX as i64,
        }
    }
}

impl Number for i32 {
    type Storage = i32;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Int32
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int32(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i32::MIN as i64,
            max: i32::MAX as i64,
        }
    }
}

impl Number for i64 {
    type Storage = i64;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Int64
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int64(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i64::MIN,
            max: i64::MAX,
        }
    }
}

impl Number for f32 {
    type Storage = f32;
    type Domain = FloatDomain;

    fn data_type() -> DataType {
        DataType::Float32
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_float32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_float32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_float().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Float32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Float32(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Float(domain)
    }

    fn full_domain() -> Self::Domain {
        FloatDomain {
            min: f32::NEG_INFINITY as f64,
            max: f32::INFINITY as f64,
        }
    }
}

impl Number for f64 {
    type Storage = f64;
    type Domain = FloatDomain;

    fn data_type() -> DataType {
        DataType::Float64
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_float64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_float64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_float().cloned()
    }
    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Float64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Float64(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Float(domain)
    }
    fn full_domain() -> Self::Domain {
        FloatDomain {
            min: f64::NEG_INFINITY,
            max: f64::INFINITY,
        }
    }
}
