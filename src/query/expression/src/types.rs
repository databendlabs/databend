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
pub mod array;
pub mod boolean;
pub mod date;
pub mod empty_array;
pub mod generic;
pub mod map;
pub mod null;
pub mod nullable;
pub mod number;
pub mod string;
pub mod timestamp;
pub mod variant;

use std::fmt::Debug;
use std::ops::Range;

use common_arrow::arrow::trusted_len::TrustedLen;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

pub use self::any::AnyType;
pub use self::array::ArrayType;
pub use self::boolean::BooleanType;
pub use self::date::DateType;
pub use self::empty_array::EmptyArrayType;
pub use self::generic::GenericType;
pub use self::map::MapType;
pub use self::null::NullType;
pub use self::nullable::NullableType;
pub use self::number::NumberDataType;
pub use self::number::NumberType;
pub use self::string::StringType;
pub use self::timestamp::TimestampType;
pub use self::variant::VariantType;
use crate::deserializations::BooleanDeserializer;
use crate::deserializations::DateDeserializer;
use crate::deserializations::EmptyArrayDeserializer;
use crate::deserializations::NullDeserializer;
use crate::deserializations::NullableDeserializer;
use crate::deserializations::NumberDeserializer;
use crate::deserializations::StringDeserializer;
use crate::deserializations::TimestampDeserializer;
use crate::deserializations::TypeDeserializer;
use crate::deserializations::VariantDeserializer;
use crate::property::Domain;
use crate::serializations::ArraySerializer;
use crate::serializations::BooleanSerializer;
use crate::serializations::DateSerializer;
use crate::serializations::EmptyArraySerializer;
use crate::serializations::NullSerializer;
use crate::serializations::NullableSerializer;
use crate::serializations::NumberSerializer;
use crate::serializations::StringSerializer;
use crate::serializations::TimestampSerializer;
use crate::serializations::TupleSerializer;
use crate::serializations::TypeSerializer;
use crate::serializations::VariantSerializer;
use crate::utils::concat_array;
use crate::values::Column;
use crate::values::Scalar;
use crate::with_number_mapped_type;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub type GenericMap = [DataType];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, EnumAsInner)]
pub enum DataType {
    Boolean,
    String,
    Number(NumberDataType),
    Timestamp,
    Date,
    Null,
    Nullable(Box<DataType>),
    EmptyArray,
    Array(Box<DataType>),
    Map(Box<DataType>),
    Tuple(Vec<DataType>),
    Variant,
    Generic(usize),
}

pub const ALL_INTEGER_TYPES: &[NumberDataType; 8] = &[
    NumberDataType::UInt8,
    NumberDataType::UInt16,
    NumberDataType::UInt32,
    NumberDataType::UInt64,
    NumberDataType::Int8,
    NumberDataType::Int16,
    NumberDataType::Int32,
    NumberDataType::Int64,
];

pub const ALL_FLOAT_TYPES: &[NumberDataType; 2] =
    &[NumberDataType::Float32, NumberDataType::Float64];
pub const ALL_NUMERICS_TYPES: &[NumberDataType; 10] =
    &concat_array(ALL_INTEGER_TYPES, ALL_FLOAT_TYPES);

impl DataType {
    pub fn wrap_nullable(&self) -> Self {
        match self {
            DataType::Nullable(_) => self.clone(),
            _ => Self::Nullable(Box::new(self.clone())),
        }
    }
    pub fn is_nullable_or_null(&self) -> bool {
        matches!(self, &DataType::Nullable(_) | &DataType::Null)
    }

    pub fn can_inside_nullable(&self) -> bool {
        !self.is_nullable_or_null()
    }

    pub fn create_serializer(&self, column: Column) -> Result<Box<dyn TypeSerializer>, String> {
        match self {
            DataType::Null => Ok(Box::new(NullSerializer::try_create(column)?)),
            DataType::Boolean => Ok(Box::new(BooleanSerializer::try_create(column)?)),
            DataType::String => Ok(Box::new(StringSerializer::try_create(column)?)),
            DataType::Number(num_ty) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE =>
                        Ok(Box::new(NumberSerializer::<NUM_TYPE>::try_create(column)?)),
                })
            }
            DataType::Date => Ok(Box::new(DateSerializer::try_create(column)?)),
            DataType::Timestamp => Ok(Box::new(TimestampSerializer::try_create(column)?)),
            DataType::Nullable(inner_ty) => {
                Ok(Box::new(NullableSerializer::try_create(column, inner_ty)?))
            }
            DataType::Array(inner_ty) => {
                Ok(Box::new(ArraySerializer::try_create(column, inner_ty)?))
            }
            DataType::EmptyArray => Ok(Box::new(EmptyArraySerializer::try_create(column)?)),
            DataType::Tuple(inner_tys) => {
                Ok(Box::new(TupleSerializer::try_create(column, inner_tys)?))
            }
            DataType::Variant => Ok(Box::new(VariantSerializer::try_create(column)?)),
            _ => unreachable!(),
        }
    }

    pub fn create_deserializer(&self) -> Box<dyn TypeDeserializer> {
        match self {
            DataType::Null => Box::new(NullDeserializer::create()),
            DataType::Boolean => Box::new(BooleanDeserializer::create()),
            DataType::String => Box::new(StringDeserializer::create()),
            DataType::Number(num_ty) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => Box::new(NumberDeserializer::<NUM_TYPE>::create()),
                })
            }
            DataType::Date => Box::new(DateDeserializer::create()),
            DataType::Timestamp => Box::new(TimestampDeserializer::create()),
            DataType::Nullable(inner_ty) => Box::new(NullableDeserializer::create(inner_ty)),
            DataType::EmptyArray => Box::new(EmptyArrayDeserializer::create()),
            DataType::Variant => Box::new(VariantDeserializer::create()),
            _ => todo!(),
        }
    }
}

pub trait ValueType: Debug + Clone + PartialEq + Sized + 'static {
    type Scalar: Debug + Clone + PartialEq;
    type ScalarRef<'a>: Debug + Clone + PartialEq;
    type Column: Debug + Clone + PartialEq;
    type Domain: Debug + Clone + PartialEq;
    type ColumnIterator<'a>: Iterator<Item = Self::ScalarRef<'a>> + TrustedLen;
    type ColumnBuilder: Debug + Clone;

    /// Upcast GAT type's lifetime.
    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short>;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar;
    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a>;

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>>;
    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column>;
    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain>;

    /// Downcast `ColumnBuilder` to a mutable reference of its inner builder type.
    ///
    /// Not every builder can be downcasted successfully.
    /// For example: `ArrayType<T: ValueType>`, `NullableType<T: ValueType>`, and `KvPair<K: ValueType, V: ValueType>`
    /// cannot be downcasted and this method will return `None`.
    ///
    /// So when using this method, we cannot unwrap the returned value directly.
    /// We should:
    ///
    /// ```ignore
    /// // builder: ColumnBuilder
    /// // T: ValueType
    /// if let Some(inner) = T::try_downcast_builder(&mut builder) {
    ///     inner.push(...);
    /// } else {
    ///     builder.push(...);
    /// }
    /// ```
    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder>;

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
    fn full_domain() -> Self::Domain;
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
