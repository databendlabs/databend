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

pub mod any;
pub mod arithmetics_type;
pub mod array;
pub mod boolean;
pub mod empty_array;
pub mod generic;
pub mod map;
pub mod null;
pub mod nullable;
pub mod number;
pub mod string;
pub mod timestamp;

use std::fmt::Debug;
use std::ops::Range;

use common_arrow::arrow::trusted_len::TrustedLen;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

pub use self::any::AnyType;
pub use self::array::ArrayType;
pub use self::boolean::BooleanType;
pub use self::empty_array::EmptyArrayType;
pub use self::generic::GenericType;
pub use self::map::MapType;
pub use self::null::NullType;
pub use self::nullable::NullableType;
pub use self::number::NumberDataType;
pub use self::number::NumberType;
pub use self::string::StringType;
pub use self::timestamp::TimestampType;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

pub type GenericMap<'a> = [DataType];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, EnumAsInner)]
pub enum DataType {
    Boolean,
    String,
    Number(NumberDataType),
    Timestamp,
    // TODO: Implement them
    // Interval,
    Null,
    Nullable(Box<DataType>),
    EmptyArray,
    Array(Box<DataType>),
    Map(Box<DataType>),
    Tuple(Vec<DataType>),
    Generic(usize),
}

pub trait ValueType: Debug + Clone + PartialEq + Sized + 'static {
    type Scalar: Debug + Clone + PartialEq;
    type ScalarRef<'a>: Debug + Clone + PartialEq;
    type Column: Debug + Clone + PartialEq;
    type Domain: Debug + Clone + PartialEq;
    type ColumnIterator<'a>: Iterator<Item = Self::ScalarRef<'a>> + TrustedLen;
    type ColumnBuilder: Debug + Clone + PartialEq;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar;
    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a>;

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>>;
    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column>;
    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain>;
    fn upcast_scalar(scalar: Self::Scalar) -> Scalar;
    fn upcast_column(col: Self::Column) -> Column;
    fn upcast_domain(domain: Self::Domain) -> Domain;

    fn column_len<'a>(col: &'a Self::Column) -> usize;
    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>>;

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a>;
    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column;
    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a>;
    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder;

    fn builder_len(builder: &Self::ColumnBuilder) -> usize;
    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>);
    fn push_default(builder: &mut Self::ColumnBuilder);
    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder);
    fn build_column(builder: Self::ColumnBuilder) -> Self::Column;
    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar;
}

pub trait ArgType: ValueType {
    fn data_type() -> DataType;
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder;

    fn column_from_vec(vec: Vec<Self::Scalar>, generics: &GenericMap) -> Self::Column {
        Self::column_from_iter(vec.iter().cloned(), generics)
    }

    fn column_from_iter(
        iter: impl Iterator<Item = Self::Scalar>,
        generics: &GenericMap,
    ) -> Self::Column {
        let mut col = Self::create_builder(iter.size_hint().0, generics);
        for item in iter {
            Self::push_item(&mut col, Self::to_scalar_ref(&item));
        }
        Self::build_column(col)
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        generics: &GenericMap,
    ) -> Self::Column {
        let mut col = Self::create_builder(iter.size_hint().0, generics);
        for item in iter {
            Self::push_item(&mut col, item);
        }
        Self::build_column(col)
    }
}
