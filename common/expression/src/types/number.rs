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
use crate::property::IntDomain;
use crate::property::UIntDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;

pub trait Number: 'static {
    type Storage: NativeType;
    type Domain: Debug + Clone;

    fn data_type() -> DataType;
    fn try_downcast_scalar(scalar: &Scalar) -> Option<Self::Storage>;
    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>>;
    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain>;
    fn upcast_scalar(scalar: Self::Storage) -> Scalar;
    fn upcast_column(col: Buffer<Self::Storage>) -> Column;
    fn upcast_domain(domain: Self::Domain) -> Domain;
    fn full_domain() -> Self::Domain;
}

pub struct NumberType<T: Number>(PhantomData<T>);

impl<Int: Number> ValueType for NumberType<Int> {
    type Scalar = Int::Storage;
    type ScalarRef<'a> = Int::Storage;
    type Column = Buffer<Int::Storage>;
    type Domain = Int::Domain;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }
}

impl<T: Number> ArgType for NumberType<T> {
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, T::Storage>>;
    type ColumnBuilder = Vec<T::Storage>;

    fn data_type() -> DataType {
        T::data_type()
    }

    fn try_downcast_scalar<'a>(scalar: &'a Scalar) -> Option<Self::ScalarRef<'a>> {
        T::try_downcast_scalar(scalar)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        T::try_downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        T::try_downcast_domain(domain)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        T::upcast_scalar(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        T::upcast_column(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        T::upcast_domain(domain)
    }

    fn full_domain(_: &GenericMap) -> Self::Domain {
        T::full_domain()
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Self::ScalarRef<'a> {
        col[index]
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().slice(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter().cloned()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
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
        builder.push(T::Storage::default());
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

impl Number for u8 {
    type Storage = u8;
    type Domain = UIntDomain;

    fn data_type() -> DataType {
        DataType::UInt8
    }

    fn try_downcast_scalar(scalar: &Scalar) -> Option<Self::Storage> {
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

    fn try_downcast_scalar(scalar: &Scalar) -> Option<Self::Storage> {
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

impl Number for i8 {
    type Storage = i8;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Int8
    }

    fn try_downcast_scalar(scalar: &Scalar) -> Option<Self::Storage> {
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

    fn try_downcast_scalar(scalar: &Scalar) -> Option<Self::Storage> {
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
