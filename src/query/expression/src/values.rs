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
use std::hash::Hash;
use std::io::Read;
use std::ops::Range;

use base64::engine::general_purpose;
use base64::prelude::*;
use common_arrow::arrow::bitmap::and;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::cast as arrow_cast;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::TimeUnit;
use common_arrow::arrow::offset::OffsetsBuffer;
use common_arrow::arrow::trusted_len::TrustedLen;
use common_exception::Result;
use common_io::prelude::BinaryRead;
use enum_as_inner::EnumAsInner;
use ethnum::i256;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use roaring::RoaringTreemap;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::property::Domain;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::boolean::BooleanDomain;
use crate::types::date::DATE_MAX;
use crate::types::date::DATE_MIN;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalColumnBuilder;
use crate::types::decimal::DecimalDataType;
use crate::types::decimal::DecimalScalar;
use crate::types::decimal::DecimalSize;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::nullable::NullableColumnVec;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberColumnBuilder;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::string::StringDomain;
use crate::types::timestamp::check_timestamp;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::variant::JSONB_NULL;
use crate::types::*;
use crate::utils::arrow::append_bitmap;
use crate::utils::arrow::bitmap_into_mut;
use crate::utils::arrow::buffer_into_mut;
use crate::utils::arrow::deserialize_column;
use crate::utils::arrow::serialize_column;
use crate::utils::FromData;
use crate::values::decimal::DecimalColumnVec;
use crate::values::map::KvPair;
use crate::with_decimal_mapped_type;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::with_number_type;

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Value<T: ValueType> {
    Scalar(T::Scalar),
    Column(T::Column),
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum ValueRef<'a, T: ValueType> {
    Scalar(T::ScalarRef<'a>),
    Column(T::Column),
}

#[derive(Debug, Clone, EnumAsInner, Eq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    Date(i32),
    Boolean(bool),
    String(Vec<u8>),
    Array(Column),
    Map(Column),
    Bitmap(Vec<u8>),
    Tuple(Vec<Scalar>),
    Variant(Vec<u8>),
}

#[derive(Clone, Default, Eq, EnumAsInner)]
pub enum ScalarRef<'a> {
    #[default]
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Boolean(bool),
    String(&'a [u8]),
    Timestamp(i64),
    Date(i32),
    Array(Column),
    Map(Column),
    Bitmap(&'a [u8]),
    Tuple(Vec<ScalarRef<'a>>),
    Variant(&'a [u8]),
}

#[derive(Clone, EnumAsInner)]
pub enum Column {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumn),
    Decimal(DecimalColumn),
    Boolean(Bitmap),
    String(StringColumn),
    Timestamp(Buffer<i64>),
    Date(Buffer<i32>),
    Array(Box<ArrayColumn<AnyType>>),
    Map(Box<ArrayColumn<AnyType>>),
    Bitmap(StringColumn),
    Nullable(Box<NullableColumn<AnyType>>),
    Tuple(Vec<Column>),
    Variant(StringColumn),
}

#[derive(Clone, EnumAsInner, Debug, PartialEq)]
pub enum ColumnVec {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberColumnVec),
    Decimal(DecimalColumnVec),
    Boolean(Vec<Bitmap>),
    String(Vec<StringColumn>),
    Timestamp(Vec<Buffer<i64>>),
    Date(Vec<Buffer<i32>>),
    Array(Vec<ArrayColumn<AnyType>>),
    Map(Vec<ArrayColumn<KvPair<AnyType, AnyType>>>),
    Bitmap(Vec<StringColumn>),
    Nullable(Box<NullableColumnVec>),
    Tuple(Vec<ColumnVec>),
    Variant(Vec<StringColumn>),
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum ColumnBuilder {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumnBuilder),
    Decimal(DecimalColumnBuilder),
    Boolean(MutableBitmap),
    String(StringColumnBuilder),
    Timestamp(Vec<i64>),
    Date(Vec<i32>),
    Array(Box<ArrayColumnBuilder<AnyType>>),
    Map(Box<ArrayColumnBuilder<AnyType>>),
    Bitmap(StringColumnBuilder),
    Nullable(Box<NullableColumnBuilder<AnyType>>),
    Tuple(Vec<ColumnBuilder>),
    Variant(StringColumnBuilder),
}

impl<'a, T: ValueType> ValueRef<'a, T> {
    pub fn to_owned(self) -> Value<T> {
        match self {
            ValueRef::Scalar(scalar) => Value::Scalar(T::to_owned_scalar(scalar)),
            ValueRef::Column(col) => Value::Column(col),
        }
    }

    pub fn semantically_eq(&'a self, other: &'a Self) -> bool {
        match (self, other) {
            (ValueRef::Scalar(s1), ValueRef::Scalar(s2)) => s1 == s2,
            (ValueRef::Column(c1), ValueRef::Column(c2)) => c1 == c2,
            (ValueRef::Scalar(s), ValueRef::Column(c))
            | (ValueRef::Column(c), ValueRef::Scalar(s)) => {
                T::iter_column(c).all(|scalar| scalar == *s)
            }
        }
    }

    pub fn len(&'a self) -> usize {
        match self {
            ValueRef::Scalar(_) => 1,
            ValueRef::Column(col) => T::column_len(col),
        }
    }

    pub fn index(&'a self, index: usize) -> Option<T::ScalarRef<'a>> {
        match self {
            ValueRef::Scalar(scalar) => Some(scalar.clone()),
            ValueRef::Column(col) => T::index_column(col, index),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&'a self, index: usize) -> T::ScalarRef<'a> {
        match self {
            ValueRef::Scalar(scalar) => scalar.clone(),
            ValueRef::Column(c) => T::index_column_unchecked(c, index),
        }
    }

    pub fn memory_size(&'a self) -> usize {
        match self {
            ValueRef::Scalar(scalar) => T::scalar_memory_size(scalar),
            ValueRef::Column(c) => T::column_memory_size(c),
        }
    }
}

impl<'a, T: ValueType> Value<T> {
    pub fn as_ref(&'a self) -> ValueRef<'a, T> {
        match self {
            Value::Scalar(scalar) => ValueRef::Scalar(T::to_scalar_ref(scalar)),
            Value::Column(col) => ValueRef::Column(col.clone()),
        }
    }

    pub fn index(&'a self, index: usize) -> Option<T::ScalarRef<'a>> {
        match self {
            Value::Scalar(scalar) => Some(T::to_scalar_ref(scalar)),
            Value::Column(col) => T::index_column(col, index),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&'a self, index: usize) -> T::ScalarRef<'a> {
        match self {
            Value::Scalar(scalar) => T::to_scalar_ref(scalar),
            Value::Column(c) => T::index_column_unchecked(c, index),
        }
    }
}

impl<T: ArgType> Value<T> {
    pub fn upcast(self) -> Value<AnyType> {
        match self {
            Value::Scalar(scalar) => Value::Scalar(T::upcast_scalar(scalar)),
            Value::Column(col) => Value::Column(T::upcast_column(col)),
        }
    }
}

impl Value<AnyType> {
    pub fn convert_to_full_column(&self, ty: &DataType, num_rows: usize) -> Column {
        match self {
            Value::Scalar(s) => {
                let builder = ColumnBuilder::repeat(&s.as_ref(), num_rows, ty);
                builder.build()
            }
            Value::Column(c) => c.clone(),
        }
    }

    pub fn try_downcast<T: ValueType>(&self) -> Option<Value<T>> {
        Some(self.as_ref().try_downcast::<T>()?.to_owned())
    }

    pub fn wrap_nullable(self, validity: Option<Bitmap>) -> Self {
        match self {
            Value::Column(c) => Value::Column(c.wrap_nullable(validity)),
            scalar => scalar,
        }
    }
}

impl<'a> ValueRef<'a, AnyType> {
    pub fn try_downcast<T: ValueType>(&self) -> Option<ValueRef<'_, T>> {
        Some(match self {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(T::try_downcast_scalar(scalar)?),
            ValueRef::Column(col) => ValueRef::Column(T::try_downcast_column(col)?),
        })
    }

    pub fn domain(&self, data_type: &DataType) -> Domain {
        match self {
            ValueRef::Scalar(scalar) => scalar.domain(data_type),
            ValueRef::Column(col) => col.domain(),
        }
    }
}

impl Scalar {
    pub fn as_ref(&self) -> ScalarRef {
        match self {
            Scalar::Null => ScalarRef::Null,
            Scalar::EmptyArray => ScalarRef::EmptyArray,
            Scalar::EmptyMap => ScalarRef::EmptyMap,
            Scalar::Number(n) => ScalarRef::Number(*n),
            Scalar::Decimal(d) => ScalarRef::Decimal(*d),
            Scalar::Boolean(b) => ScalarRef::Boolean(*b),
            Scalar::String(s) => ScalarRef::String(s.as_slice()),
            Scalar::Timestamp(t) => ScalarRef::Timestamp(*t),
            Scalar::Date(d) => ScalarRef::Date(*d),
            Scalar::Array(col) => ScalarRef::Array(col.clone()),
            Scalar::Map(col) => ScalarRef::Map(col.clone()),
            Scalar::Bitmap(b) => ScalarRef::Bitmap(b.as_slice()),
            Scalar::Tuple(fields) => ScalarRef::Tuple(fields.iter().map(Scalar::as_ref).collect()),
            Scalar::Variant(s) => ScalarRef::Variant(s.as_slice()),
        }
    }

    pub fn default_value(ty: &DataType) -> Scalar {
        match ty {
            DataType::Null => Scalar::Null,
            DataType::EmptyArray => Scalar::EmptyArray,
            DataType::EmptyMap => Scalar::EmptyMap,
            DataType::Boolean => Scalar::Boolean(false),
            DataType::String => Scalar::String(vec![]),
            DataType::Number(num_ty) => Scalar::Number(match num_ty {
                NumberDataType::UInt8 => NumberScalar::UInt8(0),
                NumberDataType::UInt16 => NumberScalar::UInt16(0),
                NumberDataType::UInt32 => NumberScalar::UInt32(0),
                NumberDataType::UInt64 => NumberScalar::UInt64(0),
                NumberDataType::Int8 => NumberScalar::Int8(0),
                NumberDataType::Int16 => NumberScalar::Int16(0),
                NumberDataType::Int32 => NumberScalar::Int32(0),
                NumberDataType::Int64 => NumberScalar::Int64(0),
                NumberDataType::Float32 => NumberScalar::Float32(OrderedFloat(0.0)),
                NumberDataType::Float64 => NumberScalar::Float64(OrderedFloat(0.0)),
            }),
            DataType::Decimal(ty) => Scalar::Decimal(ty.default_scalar()),
            DataType::Timestamp => Scalar::Timestamp(0),
            DataType::Date => Scalar::Date(0),
            DataType::Nullable(_) => Scalar::Null,
            DataType::Array(ty) => {
                let builder = ColumnBuilder::with_capacity(ty, 0);
                let col = builder.build();
                Scalar::Array(col)
            }
            DataType::Map(ty) => {
                let builder = ColumnBuilder::with_capacity(ty, 0);
                let col = builder.build();
                Scalar::Map(col)
            }
            DataType::Bitmap => Scalar::Bitmap(vec![]),
            DataType::Tuple(tys) => Scalar::Tuple(tys.iter().map(Scalar::default_value).collect()),
            DataType::Variant => Scalar::Variant(vec![]),

            _ => unimplemented!(),
        }
    }

    pub fn is_positive(&self) -> bool {
        match self {
            Scalar::Number(n) => n.is_positive(),
            Scalar::Decimal(d) => d.is_positive(),
            Scalar::Timestamp(t) => *t > 0,
            Scalar::Date(d) => *d > 0,
            _ => unreachable!("is_positive() called on non-numeric scalar"),
        }
    }
}

impl<'a> ScalarRef<'a> {
    pub fn to_owned(&self) -> Scalar {
        match self {
            ScalarRef::Null => Scalar::Null,
            ScalarRef::EmptyArray => Scalar::EmptyArray,
            ScalarRef::EmptyMap => Scalar::EmptyMap,
            ScalarRef::Number(n) => Scalar::Number(*n),
            ScalarRef::Decimal(d) => Scalar::Decimal(*d),
            ScalarRef::Boolean(b) => Scalar::Boolean(*b),
            ScalarRef::String(s) => Scalar::String(s.to_vec()),
            ScalarRef::Timestamp(t) => Scalar::Timestamp(*t),
            ScalarRef::Date(d) => Scalar::Date(*d),
            ScalarRef::Array(col) => Scalar::Array(col.clone()),
            ScalarRef::Map(col) => Scalar::Map(col.clone()),
            ScalarRef::Bitmap(b) => Scalar::Bitmap(b.to_vec()),
            ScalarRef::Tuple(fields) => {
                Scalar::Tuple(fields.iter().map(ScalarRef::to_owned).collect())
            }
            ScalarRef::Variant(s) => Scalar::Variant(s.to_vec()),
        }
    }

    pub fn domain(&self, data_type: &DataType) -> Domain {
        if !self.is_null() {
            if let DataType::Nullable(ty) = data_type {
                let domain = self.domain(ty.as_ref());
                return Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                });
            }
        }

        match self {
            ScalarRef::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ScalarRef::EmptyArray => Domain::Array(None),
            ScalarRef::EmptyMap => Domain::Map(None),
            ScalarRef::Number(num) => Domain::Number(num.domain()),
            ScalarRef::Decimal(dec) => Domain::Decimal(dec.domain()),
            ScalarRef::Boolean(true) => Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }),
            ScalarRef::Boolean(false) => Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }),
            ScalarRef::String(s) => Domain::String(StringDomain {
                min: s.to_vec(),
                max: Some(s.to_vec()),
            }),
            ScalarRef::Timestamp(t) => Domain::Timestamp(SimpleDomain { min: *t, max: *t }),
            ScalarRef::Date(d) => Domain::Date(SimpleDomain { min: *d, max: *d }),
            ScalarRef::Array(array) => {
                if array.len() == 0 {
                    Domain::Array(None)
                } else {
                    Domain::Array(Some(Box::new(array.domain())))
                }
            }
            ScalarRef::Map(map) => {
                if map.len() == 0 {
                    Domain::Map(None)
                } else {
                    let inner_domain = map.domain();
                    let map_domain = match inner_domain {
                        Domain::Tuple(domains) => {
                            (Box::new(domains[0].clone()), Box::new(domains[1].clone()))
                        }
                        _ => unreachable!(),
                    };
                    Domain::Map(Some(map_domain))
                }
            }
            ScalarRef::Tuple(fields) => {
                let types = data_type.as_tuple().unwrap();
                Domain::Tuple(
                    fields
                        .iter()
                        .zip(types.iter())
                        .map(|(field, data_type)| field.domain(data_type))
                        .collect(),
                )
            }
            ScalarRef::Bitmap(_) | ScalarRef::Variant(_) => Domain::Undefined,
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            ScalarRef::Null | ScalarRef::EmptyArray | ScalarRef::EmptyMap => 0,
            ScalarRef::Number(NumberScalar::UInt8(_)) => 1,
            ScalarRef::Number(NumberScalar::UInt16(_)) => 2,
            ScalarRef::Number(NumberScalar::UInt32(_)) => 4,
            ScalarRef::Number(NumberScalar::UInt64(_)) => 8,
            ScalarRef::Number(NumberScalar::Float32(_)) => 4,
            ScalarRef::Number(NumberScalar::Float64(_)) => 8,
            ScalarRef::Number(NumberScalar::Int8(_)) => 1,
            ScalarRef::Number(NumberScalar::Int16(_)) => 2,
            ScalarRef::Number(NumberScalar::Int32(_)) => 4,
            ScalarRef::Number(NumberScalar::Int64(_)) => 8,
            ScalarRef::Decimal(DecimalScalar::Decimal128(_, _)) => 16,
            ScalarRef::Decimal(DecimalScalar::Decimal256(_, _)) => 32,
            ScalarRef::Boolean(_) => 1,
            ScalarRef::String(s) => s.len(),
            ScalarRef::Timestamp(_) => 8,
            ScalarRef::Date(_) => 4,
            ScalarRef::Array(col) => col.memory_size(),
            ScalarRef::Map(col) => col.memory_size(),
            ScalarRef::Bitmap(b) => b.len(),
            ScalarRef::Tuple(scalars) => scalars.iter().map(|s| s.memory_size()).sum(),
            ScalarRef::Variant(buf) => buf.len(),
        }
    }

    /// Infer the data type of the scalar.
    /// If the scalar is Null, the data type is `DataType::Null`,
    /// otherwise, the inferred data type is non-nullable.
    pub fn infer_data_type(&self) -> DataType {
        match self {
            ScalarRef::Null => DataType::Null,
            ScalarRef::EmptyArray => DataType::EmptyArray,
            ScalarRef::EmptyMap => DataType::EmptyMap,
            ScalarRef::Number(s) => DataType::Number(s.data_type()),
            ScalarRef::Decimal(s) => with_decimal_type!(|DECIMAL_TYPE| match s {
                DecimalScalar::DECIMAL_TYPE(_, size) =>
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size)),
            }),
            ScalarRef::Boolean(_) => DataType::Boolean,
            ScalarRef::String(_) => DataType::String,
            ScalarRef::Timestamp(_) => DataType::Timestamp,
            ScalarRef::Date(_) => DataType::Date,
            ScalarRef::Array(array) => DataType::Array(Box::new(array.data_type())),
            ScalarRef::Map(col) => DataType::Map(Box::new(col.data_type())),
            ScalarRef::Bitmap(_) => DataType::Bitmap,
            ScalarRef::Tuple(fields) => {
                let inner = fields
                    .iter()
                    .map(|field| field.infer_data_type())
                    .collect::<Vec<_>>();
                DataType::Tuple(inner)
            }
            ScalarRef::Variant(_) => DataType::Variant,
        }
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
            (Scalar::EmptyArray, Scalar::EmptyArray) => Some(Ordering::Equal),
            (Scalar::EmptyMap, Scalar::EmptyMap) => Some(Ordering::Equal),
            (Scalar::Number(n1), Scalar::Number(n2)) => n1.partial_cmp(n2),
            (Scalar::Decimal(d1), Scalar::Decimal(d2)) => d1.partial_cmp(d2),
            (Scalar::Boolean(b1), Scalar::Boolean(b2)) => b1.partial_cmp(b2),
            (Scalar::String(s1), Scalar::String(s2)) => s1.partial_cmp(s2),
            (Scalar::Timestamp(t1), Scalar::Timestamp(t2)) => t1.partial_cmp(t2),
            (Scalar::Date(d1), Scalar::Date(d2)) => d1.partial_cmp(d2),
            (Scalar::Array(a1), Scalar::Array(a2)) => a1.partial_cmp(a2),
            (Scalar::Map(m1), Scalar::Map(m2)) => m1.partial_cmp(m2),
            (Scalar::Bitmap(b1), Scalar::Bitmap(b2)) => b1.partial_cmp(b2),
            (Scalar::Tuple(t1), Scalar::Tuple(t2)) => t1.partial_cmp(t2),
            (Scalar::Variant(v1), Scalar::Variant(v2)) => {
                jsonb::compare(v1.as_slice(), v2.as_slice()).ok()
            }
            _ => None,
        }
    }
}

impl Ord for Scalar {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for ScalarRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ScalarRef::Null, ScalarRef::Null) => Some(Ordering::Equal),
            (ScalarRef::EmptyArray, ScalarRef::EmptyArray) => Some(Ordering::Equal),
            (ScalarRef::EmptyMap, ScalarRef::EmptyMap) => Some(Ordering::Equal),
            (ScalarRef::Number(n1), ScalarRef::Number(n2)) => n1.partial_cmp(n2),
            (ScalarRef::Decimal(d1), ScalarRef::Decimal(d2)) => d1.partial_cmp(d2),
            (ScalarRef::Boolean(b1), ScalarRef::Boolean(b2)) => b1.partial_cmp(b2),
            (ScalarRef::String(s1), ScalarRef::String(s2)) => s1.partial_cmp(s2),
            (ScalarRef::Timestamp(t1), ScalarRef::Timestamp(t2)) => t1.partial_cmp(t2),
            (ScalarRef::Date(d1), ScalarRef::Date(d2)) => d1.partial_cmp(d2),
            (ScalarRef::Array(a1), ScalarRef::Array(a2)) => a1.partial_cmp(a2),
            (ScalarRef::Map(m1), ScalarRef::Map(m2)) => m1.partial_cmp(m2),
            (ScalarRef::Bitmap(b1), ScalarRef::Bitmap(b2)) => b1.partial_cmp(b2),
            (ScalarRef::Tuple(t1), ScalarRef::Tuple(t2)) => t1.partial_cmp(t2),
            (ScalarRef::Variant(v1), ScalarRef::Variant(v2)) => jsonb::compare(v1, v2).ok(),
            _ => None,
        }
    }
}

impl Ord for ScalarRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for ScalarRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl Hash for ScalarRef<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ScalarRef::Null | ScalarRef::EmptyArray | ScalarRef::EmptyMap => {}
            ScalarRef::Number(t) => with_number_type!(|NUM_TYPE| match t {
                NumberScalar::NUM_TYPE(v) => {
                    v.hash(state);
                }
            }),
            ScalarRef::Decimal(t) => with_decimal_type!(|DECIMAL_TYPE| match t {
                DecimalScalar::DECIMAL_TYPE(v, _) => {
                    v.hash(state);
                }
            }),
            ScalarRef::Boolean(v) => v.hash(state),
            ScalarRef::String(v) => v.hash(state),
            ScalarRef::Timestamp(v) => v.hash(state),
            ScalarRef::Date(v) => v.hash(state),
            ScalarRef::Array(v) => {
                let str = serialize_column(v);
                str.hash(state);
            }
            ScalarRef::Map(v) => {
                let str = serialize_column(v);
                str.hash(state);
            }
            ScalarRef::Bitmap(v) => v.hash(state),
            ScalarRef::Tuple(v) => {
                v.hash(state);
            }
            ScalarRef::Variant(v) => v.hash(state),
        }
    }
}

impl Hash for Scalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Column::Null { len: col1 }, Column::Null { len: col2 }) => col1.partial_cmp(col2),
            (Column::EmptyArray { len: col1 }, Column::EmptyArray { len: col2 }) => {
                col1.partial_cmp(col2)
            }
            (Column::EmptyMap { len: col1 }, Column::EmptyMap { len: col2 }) => {
                col1.partial_cmp(col2)
            }
            (Column::Number(col1), Column::Number(col2)) => col1.partial_cmp(col2),
            (Column::Decimal(col1), Column::Decimal(col2)) => col1.partial_cmp(col2),
            (Column::Boolean(col1), Column::Boolean(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::String(col1), Column::String(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Timestamp(col1), Column::Timestamp(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Date(col1), Column::Date(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Array(col1), Column::Array(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Map(col1), Column::Map(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Bitmap(col1), Column::Bitmap(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Nullable(col1), Column::Nullable(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Tuple(fields1), Column::Tuple(fields2)) => fields1.partial_cmp(fields2),
            (Column::Variant(col1), Column::Variant(col2)) => col1
                .iter()
                .partial_cmp_by(col2.iter(), |v1, v2| jsonb::compare(v1, v2).ok()),
            _ => None,
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

pub const EXTENSION_KEY: &str = "Extension";
pub const ARROW_EXT_TYPE_EMPTY_ARRAY: &str = "EmptyArray";
pub const ARROW_EXT_TYPE_EMPTY_MAP: &str = "EmptyMap";
pub const ARROW_EXT_TYPE_VARIANT: &str = "Variant";
pub const ARROW_EXT_TYPE_BITMAP: &str = "Bitmap";

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Null { len } => *len,
            Column::EmptyArray { len } => *len,
            Column::EmptyMap { len } => *len,
            Column::Number(col) => col.len(),
            Column::Decimal(col) => col.len(),
            Column::Boolean(col) => col.len(),
            Column::String(col) => col.len(),
            Column::Timestamp(col) => col.len(),
            Column::Date(col) => col.len(),
            Column::Array(col) => col.len(),
            Column::Map(col) => col.len(),
            Column::Bitmap(col) => col.len(),
            Column::Nullable(col) => col.len(),
            Column::Tuple(fields) => fields[0].len(),
            Column::Variant(col) => col.len(),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef> {
        match self {
            Column::Null { .. } => Some(ScalarRef::Null),
            Column::EmptyArray { .. } => Some(ScalarRef::EmptyArray),
            Column::EmptyMap { .. } => Some(ScalarRef::EmptyMap),
            Column::Number(col) => Some(ScalarRef::Number(col.index(index)?)),
            Column::Decimal(col) => Some(ScalarRef::Decimal(col.index(index)?)),
            Column::Boolean(col) => Some(ScalarRef::Boolean(col.get(index)?)),
            Column::String(col) => Some(ScalarRef::String(col.index(index)?)),
            Column::Timestamp(col) => Some(ScalarRef::Timestamp(col.get(index).cloned()?)),
            Column::Date(col) => Some(ScalarRef::Date(col.get(index).cloned()?)),
            Column::Array(col) => Some(ScalarRef::Array(col.index(index)?)),
            Column::Map(col) => Some(ScalarRef::Map(col.index(index)?)),
            Column::Bitmap(col) => Some(ScalarRef::Bitmap(col.index(index)?)),
            Column::Nullable(col) => Some(col.index(index)?.unwrap_or(ScalarRef::Null)),
            Column::Tuple(fields) => Some(ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index(index))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Column::Variant(col) => Some(ScalarRef::Variant(col.index(index)?)),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef {
        match self {
            Column::Null { .. } => ScalarRef::Null,
            Column::EmptyArray { .. } => ScalarRef::EmptyArray,
            Column::EmptyMap { .. } => ScalarRef::EmptyMap,
            Column::Number(col) => ScalarRef::Number(col.index_unchecked(index)),
            Column::Decimal(col) => ScalarRef::Decimal(col.index_unchecked(index)),
            Column::Boolean(col) => ScalarRef::Boolean(col.get_bit_unchecked(index)),
            Column::String(col) => ScalarRef::String(col.index_unchecked(index)),
            Column::Timestamp(col) => ScalarRef::Timestamp(*col.get_unchecked(index)),
            Column::Date(col) => ScalarRef::Date(*col.get_unchecked(index)),
            Column::Array(col) => ScalarRef::Array(col.index_unchecked(index)),
            Column::Map(col) => ScalarRef::Map(col.index_unchecked(index)),
            Column::Bitmap(col) => ScalarRef::Bitmap(col.index_unchecked(index)),
            Column::Nullable(col) => col.index_unchecked(index).unwrap_or(ScalarRef::Null),
            Column::Tuple(fields) => ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index_unchecked(index))
                    .collect::<Vec<_>>(),
            ),
            Column::Variant(col) => ScalarRef::Variant(col.index_unchecked(index)),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );

        if range.is_empty() {
            let builder = ColumnBuilder::with_capacity(&self.data_type(), 0);
            return builder.build();
        }

        match self {
            Column::Null { .. } => Column::Null {
                len: range.end - range.start,
            },
            Column::EmptyArray { .. } => Column::EmptyArray {
                len: range.end - range.start,
            },
            Column::EmptyMap { .. } => Column::EmptyMap {
                len: range.end - range.start,
            },
            Column::Number(col) => Column::Number(col.slice(range)),
            Column::Decimal(col) => Column::Decimal(col.slice(range)),
            Column::Boolean(col) => {
                Column::Boolean(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::String(col) => Column::String(col.slice(range)),
            Column::Timestamp(col) => {
                Column::Timestamp(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Date(col) => {
                Column::Date(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Array(col) => Column::Array(Box::new(col.slice(range))),
            Column::Map(col) => Column::Map(Box::new(col.slice(range))),
            Column::Bitmap(col) => Column::Bitmap(col.slice(range)),
            Column::Nullable(col) => Column::Nullable(Box::new(col.slice(range))),
            Column::Tuple(fields) => Column::Tuple(
                fields
                    .iter()
                    .map(|field| field.slice(range.clone()))
                    .collect(),
            ),
            Column::Variant(col) => Column::Variant(col.slice(range)),
        }
    }

    pub fn iter(&self) -> ColumnIterator {
        ColumnIterator {
            column: self,
            index: 0,
            len: self.len(),
        }
    }

    pub fn domain(&self) -> Domain {
        if !matches!(self, Column::Array(_) | Column::Map(_)) {
            assert!(self.len() > 0);
        }
        match self {
            Column::Null { .. } => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Column::EmptyArray { .. } => Domain::Array(None),
            Column::EmptyMap { .. } => Domain::Undefined,
            Column::Number(col) => Domain::Number(col.domain()),
            Column::Decimal(col) => Domain::Decimal(col.domain()),
            Column::Boolean(col) => Domain::Boolean(BooleanDomain {
                has_false: col.unset_bits() > 0,
                has_true: col.len() - col.unset_bits() > 0,
            }),
            Column::String(col) => {
                let (min, max) = StringType::iter_column(col).minmax().into_option().unwrap();
                Domain::String(StringDomain {
                    min: min.to_vec(),
                    max: Some(max.to_vec()),
                })
            }
            Column::Timestamp(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Timestamp(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Date(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Date(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Array(col) => {
                if col.len() == 0 || col.values.len() == 0 {
                    Domain::Array(None)
                } else {
                    let inner_domain = col.values.domain();
                    Domain::Array(Some(Box::new(inner_domain)))
                }
            }
            Column::Map(col) => {
                if col.len() == 0 || col.values.len() == 0 {
                    Domain::Map(None)
                } else {
                    let inner_domain = col.values.domain();
                    let map_domain = match inner_domain {
                        Domain::Tuple(domains) => {
                            (Box::new(domains[0].clone()), Box::new(domains[1].clone()))
                        }
                        _ => unreachable!(),
                    };
                    Domain::Map(Some(map_domain))
                }
            }
            Column::Nullable(col) => {
                let inner_domain = col.column.domain();
                Domain::Nullable(NullableDomain {
                    has_null: col.validity.unset_bits() > 0,
                    value: Some(Box::new(inner_domain)),
                })
            }
            Column::Tuple(fields) => {
                let domains = fields.iter().map(|col| col.domain()).collect::<Vec<_>>();
                Domain::Tuple(domains)
            }
            Column::Bitmap(_) | Column::Variant(_) => Domain::Undefined,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Column::Null { .. } => DataType::Null,
            Column::EmptyArray { .. } => DataType::EmptyArray,
            Column::EmptyMap { .. } => DataType::EmptyMap,
            Column::Number(c) => with_number_type!(|NUM_TYPE| match c {
                NumberColumn::NUM_TYPE(_) => DataType::Number(NumberDataType::NUM_TYPE),
            }),
            Column::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumn::DECIMAL_TYPE(_, size) =>
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size)),
            }),
            Column::Boolean(_) => DataType::Boolean,
            Column::String(_) => DataType::String,
            Column::Timestamp(_) => DataType::Timestamp,
            Column::Date(_) => DataType::Date,
            Column::Array(array) => {
                let inner = array.values.data_type();
                DataType::Array(Box::new(inner))
            }
            Column::Map(col) => {
                let inner = col.values.data_type();
                DataType::Map(Box::new(inner))
            }
            Column::Bitmap(_) => DataType::Bitmap,
            Column::Nullable(inner) => {
                let inner = inner.column.data_type();
                inner.wrap_nullable()
            }
            Column::Tuple(fields) => {
                let inner = fields.iter().map(|col| col.data_type()).collect::<Vec<_>>();
                DataType::Tuple(inner)
            }
            Column::Variant(_) => DataType::Variant,
        }
    }

    pub fn arrow_field(&self) -> common_arrow::arrow::datatypes::Field {
        use common_arrow::arrow::datatypes::DataType as ArrowDataType;
        use common_arrow::arrow::datatypes::Field as ArrowField;
        let dummy = "DUMMY".to_string();
        let is_nullable = matches!(&self, Column::Nullable(_));
        let arrow_type: ArrowDataType = (&self.data_type()).into();
        ArrowField::new(dummy, arrow_type, is_nullable)
    }

    pub fn as_arrow(&self) -> Box<dyn common_arrow::arrow::array::Array> {
        let arrow_type = self.arrow_field().data_type().clone();
        match self {
            Column::Null { len } => Box::new(common_arrow::arrow::array::NullArray::new_null(
                arrow_type, *len,
            )),
            Column::EmptyArray { len } => Box::new(
                common_arrow::arrow::array::NullArray::new_null(arrow_type, *len),
            ),
            Column::EmptyMap { len } => Box::new(common_arrow::arrow::array::NullArray::new_null(
                arrow_type, *len,
            )),
            Column::Number(NumberColumn::UInt8(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u8>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::UInt16(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u16>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::UInt32(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u32>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::UInt64(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u64>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::Int8(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i8>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::Int16(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i16>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::Int32(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i32>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::Int64(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i64>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Number(NumberColumn::Float32(col)) => {
                let values =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(col.clone()) };
                Box::new(
                    common_arrow::arrow::array::PrimitiveArray::<f32>::try_new(
                        arrow_type, values, None,
                    )
                    .unwrap(),
                )
            }
            Column::Number(NumberColumn::Float64(col)) => {
                let values =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(col.clone()) };
                Box::new(
                    common_arrow::arrow::array::PrimitiveArray::<f64>::try_new(
                        arrow_type, values, None,
                    )
                    .unwrap(),
                )
            }
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i128>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => {
                let values = unsafe { std::mem::transmute(col.clone()) };
                Box::new(
                    common_arrow::arrow::array::PrimitiveArray::<common_arrow::arrow::types::i256>::try_new(
                        arrow_type,
                        values,
                        None,
                    )
                        .unwrap()
                )
            }
            Column::Boolean(col) => Box::new(
                common_arrow::arrow::array::BooleanArray::try_new(arrow_type, col.clone(), None)
                    .unwrap(),
            ),
            Column::String(col) => {
                let offsets: Buffer<i64> =
                    col.offsets().iter().map(|offset| *offset as i64).collect();
                Box::new(
                    common_arrow::arrow::array::BinaryArray::<i64>::try_new(
                        arrow_type,
                        unsafe { OffsetsBuffer::new_unchecked(offsets) },
                        col.data().clone(),
                        None,
                    )
                    .unwrap(),
                )
            }

            Column::Timestamp(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i64>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Date(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i32>::try_new(
                    arrow_type,
                    col.clone(),
                    None,
                )
                .unwrap(),
            ),
            Column::Array(col) => {
                let offsets: Buffer<i64> =
                    col.offsets.iter().map(|offset| *offset as i64).collect();
                Box::new(
                    common_arrow::arrow::array::ListArray::<i64>::try_new(
                        arrow_type,
                        unsafe { OffsetsBuffer::new_unchecked(offsets) },
                        col.values.as_arrow(),
                        None,
                    )
                    .unwrap(),
                )
            }
            Column::Map(col) => {
                let offsets: Buffer<i32> =
                    col.offsets.iter().map(|offset| *offset as i32).collect();
                let values = match (&arrow_type, &col.values) {
                    (ArrowType::Map(inner_field, _), Column::Tuple(fields)) => {
                        let inner_type = inner_field.data_type.clone();
                        Box::new(
                            common_arrow::arrow::array::StructArray::try_new(
                                inner_type,
                                fields.iter().map(|field| field.as_arrow()).collect(),
                                None,
                            )
                            .unwrap(),
                        )
                    }
                    (_, _) => unreachable!(),
                };
                Box::new(
                    common_arrow::arrow::array::MapArray::try_new(
                        arrow_type,
                        unsafe { OffsetsBuffer::new_unchecked(offsets) },
                        values,
                        None,
                    )
                    .unwrap(),
                )
            }
            Column::Bitmap(col) => {
                let offsets: Buffer<i64> =
                    col.offsets().iter().map(|offset| *offset as i64).collect();
                Box::new(
                    common_arrow::arrow::array::BinaryArray::<i64>::try_new(
                        arrow_type,
                        unsafe { OffsetsBuffer::new_unchecked(offsets) },
                        col.data().clone(),
                        None,
                    )
                    .unwrap(),
                )
            }
            Column::Nullable(col) => {
                let arrow_array = col.column.as_arrow();
                Self::set_validity(arrow_array.clone(), &col.validity)
            }
            Column::Tuple(fields) => Box::new(
                common_arrow::arrow::array::StructArray::try_new(
                    arrow_type,
                    fields.iter().map(|field| field.as_arrow()).collect(),
                    None,
                )
                .unwrap(),
            ),
            Column::Variant(col) => {
                let offsets: Buffer<i64> =
                    col.offsets().iter().map(|offset| *offset as i64).collect();
                Box::new(
                    common_arrow::arrow::array::BinaryArray::<i64>::try_new(
                        arrow_type,
                        unsafe { OffsetsBuffer::new_unchecked(offsets) },
                        col.data().clone(),
                        None,
                    )
                    .unwrap(),
                )
            }
        }
    }

    pub fn set_validity(
        arrow_array: Box<dyn common_arrow::arrow::array::Array>,
        validity: &Bitmap,
    ) -> Box<dyn common_arrow::arrow::array::Array> {
        // merge Struct validity with the inner fields validity
        let validity = match arrow_array.validity() {
            Some(inner_validity) => and(inner_validity, validity),
            None => validity.clone(),
        };

        match arrow_array.data_type() {
            ArrowType::Null => arrow_array.clone(),
            ArrowType::Extension(_, t, _) if **t == ArrowType::Null => arrow_array.clone(),
            ArrowType::Struct(_) => {
                let struct_array = arrow_array
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::StructArray>()
                    .expect("fail to read from arrow: array should be `StructArray`");
                let fields = struct_array
                    .values()
                    .iter()
                    .map(|array| {
                        let array = Self::set_validity(array.clone(), &validity);
                        array.clone()
                    })
                    .collect::<Vec<_>>();
                Box::new(
                    common_arrow::arrow::array::StructArray::try_new(
                        arrow_array.data_type().clone(),
                        fields,
                        Some(validity),
                    )
                    .unwrap(),
                )
            }
            _ => arrow_array.with_validity(Some(validity)),
        }
    }

    pub fn from_arrow(
        arrow_col: &dyn common_arrow::arrow::array::Array,
        data_type: &DataType,
    ) -> Column {
        use common_arrow::arrow::datatypes::DataType as ArrowDataType;

        let is_nullable = data_type.is_nullable();
        let data_type = data_type.remove_nullable();
        let column = match arrow_col.data_type() {
            ArrowDataType::Null => Column::Null {
                len: arrow_col.len(),
            },
            ArrowDataType::Extension(name, _, _) if name == ARROW_EXT_TYPE_EMPTY_ARRAY => {
                Column::EmptyArray {
                    len: arrow_col.len(),
                }
            }
            ArrowDataType::Extension(name, _, _) if name == ARROW_EXT_TYPE_EMPTY_MAP => {
                Column::EmptyMap {
                    len: arrow_col.len(),
                }
            }
            ArrowDataType::UInt8 => Column::Number(NumberColumn::UInt8(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt8Array>()
                    .expect("fail to read from arrow: array should be `UInt8Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::UInt16 => Column::Number(NumberColumn::UInt16(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt16Array>()
                    .expect("fail to read from arrow: array should be `UInt16Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::UInt32 => Column::Number(NumberColumn::UInt32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt32Array>()
                    .expect("fail to read from arrow: array should be `UInt32Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::UInt64 => Column::Number(NumberColumn::UInt64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt64Array>()
                    .expect("fail to read from arrow: array should be `UInt64Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int8 => Column::Number(NumberColumn::Int8(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int8Array>()
                    .expect("fail to read from arrow: array should be `Int8Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int16 => Column::Number(NumberColumn::Int16(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int16Array>()
                    .expect("fail to read from arrow: array should be `Int16Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int32 => Column::Number(NumberColumn::Int32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int32Array>()
                    .expect("fail to read from arrow: array should be `Int32Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int64 => Column::Number(NumberColumn::Int64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int64Array>()
                    .expect("fail to read from arrow: array should be `Int64Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Float32 => {
                let col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Float32Array>()
                    .expect("fail to read from arrow: array should be `Float32Array`")
                    .values()
                    .clone();
                let col = unsafe { std::mem::transmute::<Buffer<f32>, Buffer<F32>>(col) };
                Column::Number(NumberColumn::Float32(col))
            }
            ArrowDataType::Float64 => {
                let col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Float64Array>()
                    .expect("fail to read from arrow: array should be `Float64Array`")
                    .values()
                    .clone();
                let col = unsafe { std::mem::transmute::<Buffer<f64>, Buffer<F64>>(col) };
                Column::Number(NumberColumn::Float64(col))
            }
            ArrowDataType::Boolean => Column::Boolean(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BooleanArray>()
                    .expect("fail to read from arrow: array should be `BooleanArray`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::LargeBinary => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BinaryArray<i64>>()
                    .expect("fail to read from arrow: array should be `BinaryArray<i64>`");
                let offsets = arrow_col.offsets().clone().into_inner();

                let offsets = unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                if data_type.is_variant() {
                    // Variant column from udf server is converted to LargeBinary, we restore it back here.
                    Column::Variant(StringColumn::new(arrow_col.values().clone(), offsets))
                } else {
                    Column::String(StringColumn::new(arrow_col.values().clone(), offsets))
                }
            }
            // TODO: deprecate it and use LargeBinary instead
            ArrowDataType::Binary => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BinaryArray<i32>>()
                    .expect("fail to read from arrow: array should be `BinaryArray<i32>`");
                let offsets = arrow_col
                    .offsets()
                    .buffer()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();

                Column::String(StringColumn::new(
                    arrow_col.values().clone(),
                    offsets.into(),
                ))
            }

            ArrowDataType::FixedSizeBinary(size) => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::FixedSizeBinaryArray>()
                    .expect("fail to read from arrow: array should be `FixedSizeBinaryArray`");

                let offsets = (0..arrow_col.len() as u64 + 1)
                    .map(|x| x * (*size) as u64)
                    .collect::<Vec<_>>();

                Column::String(StringColumn::new(
                    arrow_col.values().clone(),
                    offsets.into(),
                ))
            }

            // TODO: deprecate it and use LargeBinary instead
            ArrowDataType::Utf8 => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Utf8Array<i32>>()
                    .expect("fail to read from arrow: array should be `Utf8Array<i32>`");
                let offsets = arrow_col
                    .offsets()
                    .buffer()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();

                Column::String(StringColumn::new(
                    arrow_col.values().clone(),
                    offsets.into(),
                ))
            }
            // TODO: deprecate it and use LargeBinary instead
            ArrowDataType::LargeUtf8 => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Utf8Array<i64>>()
                    .expect("fail to read from arrow: array should be `Utf8Array<i64>`");
                let offsets = arrow_col
                    .offsets()
                    .buffer()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::String(StringColumn::new(
                    arrow_col.values().clone(),
                    offsets.into(),
                ))
            }

            ArrowType::Timestamp(uint, _) => {
                let values = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int64Array>()
                    .expect("fail to read from arrow: array should be `Int64Array`")
                    .values();
                let convert = match uint {
                    TimeUnit::Second => (1_000_000, 1),
                    TimeUnit::Millisecond => (1_000, 1),
                    TimeUnit::Microsecond => (1, 1),
                    TimeUnit::Nanosecond => (1, 1_000),
                };

                let values = if convert.0 == 1 && convert.1 == 1 {
                    values.clone()
                } else {
                    let values = values
                        .iter()
                        .map(|x| x * convert.0 / convert.1)
                        .collect::<Vec<_>>();
                    values.into()
                };
                Column::Timestamp(values)
            }
            ArrowDataType::Date32 => Column::Date(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int32Array>()
                    .expect("fail to read from arrow: array should be `Int32Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Extension(name, box ty, None) if name == ARROW_EXT_TYPE_VARIANT => {
                match ty {
                    ArrowDataType::LargeBinary => {
                        let arrow_col = arrow_col
                            .as_any()
                            .downcast_ref::<common_arrow::arrow::array::BinaryArray<i64>>()
                            .expect("fail to read from arrow: array should be `BinaryArray<i64>`");
                        let offsets = arrow_col.offsets().clone().into_inner();

                        let offsets =
                            unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };

                        Column::Variant(StringColumn::new(arrow_col.values().clone(), offsets))
                    }
                    ArrowDataType::Binary => {
                        let arrow_col = arrow_col
                            .as_any()
                            .downcast_ref::<common_arrow::arrow::array::BinaryArray<i32>>()
                            .expect("fail to read from arrow: array should be `BinaryArray<i32>`");
                        let offsets = arrow_col
                            .offsets()
                            .buffer()
                            .iter()
                            .map(|x| *x as u64)
                            .collect::<Vec<_>>();
                        Column::Variant(StringColumn::new(
                            arrow_col.values().clone(),
                            offsets.into(),
                        ))
                    }
                    _ => unreachable!(
                        "fail to read from arrow: array should be `BinaryArray<i32>` or `BinaryArray<i64>`"
                    ),
                }
            }
            ArrowDataType::List(f) => {
                let array_list = arrow_cast::cast(
                    arrow_col,
                    &ArrowDataType::LargeList(f.clone()),
                    arrow_cast::CastOptions {
                        wrapped: true,
                        partial: true,
                    },
                )
                .expect("list to large list cast should be ok");

                let values_col = array_list
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::ListArray<i64>>()
                    .expect("fail to read from arrow: array should be `ListArray<i64>`");

                let array_type = data_type.as_array().unwrap();
                let values = Column::from_arrow(&**values_col.values(), array_type.as_ref());
                let offsets = values_col
                    .offsets()
                    .buffer()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::Array(Box::new(ArrayColumn {
                    values,
                    offsets: offsets.into(),
                }))
            }
            ArrowDataType::LargeList(_) => {
                let values_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::ListArray<i64>>()
                    .expect("fail to read from arrow: array should be `ListArray<i64>`");

                let array_type = data_type.as_array().unwrap();
                let values = Column::from_arrow(&**values_col.values(), array_type.as_ref());
                let offsets = values_col
                    .offsets()
                    .buffer()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::Array(Box::new(ArrayColumn {
                    values,
                    offsets: offsets.into(),
                }))
            }
            ArrowDataType::Map(_, _) => {
                let map_type = data_type.as_map().unwrap();
                let map_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::MapArray>()
                    .expect("fail to read from arrow: array should be `MapArray`");

                let values = Column::from_arrow(&**map_col.field(), map_type.as_ref());
                let offsets = map_col
                    .offsets()
                    .buffer()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::Map(Box::new(ArrayColumn {
                    values,
                    offsets: offsets.into(),
                }))
            }
            ArrowDataType::Struct(_) => {
                let struct_type = data_type.as_tuple().unwrap();
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::StructArray>()
                    .expect("fail to read from arrow: array should be `StructArray`");
                let fields = arrow_col
                    .values()
                    .iter()
                    .zip(struct_type.iter())
                    .map(|(field, dt)| Column::from_arrow(&**field, dt))
                    .collect::<Vec<_>>();
                Column::Tuple(fields)
            }
            ArrowDataType::Decimal(precision, scale) => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::PrimitiveArray<i128>>()
                    .expect("fail to read from arrow: array should be `DecimalArray`");
                Column::Decimal(DecimalColumn::Decimal128(
                    arrow_col.values().clone(),
                    DecimalSize {
                        precision: *precision as u8,
                        scale: *scale as u8,
                    },
                ))
            }
            ArrowDataType::Decimal256(precision, scale) => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::PrimitiveArray<common_arrow::arrow::types::i256>>()
                    .expect("fail to read from arrow: array should be `DecimalArray`");

                let values = unsafe { std::mem::transmute(arrow_col.values().clone()) };
                Column::Decimal(DecimalColumn::Decimal256(values, DecimalSize {
                    precision: *precision as u8,
                    scale: *scale as u8,
                }))
            }
            ArrowDataType::Extension(name, box ty, None) if name == ARROW_EXT_TYPE_BITMAP => {
                match ty {
                    ArrowDataType::LargeBinary => {
                        let arrow_col = arrow_col
                            .as_any()
                            .downcast_ref::<common_arrow::arrow::array::BinaryArray<i64>>()
                            .expect("fail to read from arrow: array should be `BinaryArray<i64>`");
                        let offsets = arrow_col.offsets().clone().into_inner();

                        let offsets =
                            unsafe { std::mem::transmute::<Buffer<i64>, Buffer<u64>>(offsets) };
                        Column::Bitmap(StringColumn::new(arrow_col.values().clone(), offsets))
                    }
                    ArrowDataType::Binary => {
                        let arrow_col = arrow_col
                            .as_any()
                            .downcast_ref::<common_arrow::arrow::array::BinaryArray<i32>>()
                            .expect("fail to read from arrow: array should be `BinaryArray<i32>`");
                        let offsets = arrow_col
                            .offsets()
                            .buffer()
                            .iter()
                            .map(|x| *x as u64)
                            .collect::<Vec<_>>();
                        Column::Bitmap(StringColumn::new(
                            arrow_col.values().clone(),
                            offsets.into(),
                        ))
                    }
                    _ => unreachable!(
                        "fail to read from arrow: array should be `BinaryArray<i32>` or `BinaryArray<i64>`"
                    ),
                }
            }
            ty => unimplemented!("unsupported arrow type {ty:?}"),
        };

        if is_nullable {
            let validity = arrow_col
                .validity()
                .cloned()
                .unwrap_or_else(|| Bitmap::new_constant(true, arrow_col.len()));
            Column::Nullable(Box::new(NullableColumn { column, validity }))
        } else {
            column
        }
    }

    pub fn random(ty: &DataType, len: usize) -> Self {
        use rand::distributions::Alphanumeric;
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;

        // Migrate from legacy code:
        match ty {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            DataType::Boolean => {
                BooleanType::from_data((0..len).map(|_| SmallRng::from_entropy().gen_bool(0.5)))
            }
            DataType::String => StringType::from_data((0..len).map(|_| {
                let rng = SmallRng::from_entropy();
                rng.sample_iter(&Alphanumeric)
                    // randomly generate 5 characters.
                    .take(5)
                    .map(u8::from)
                    .collect::<Vec<_>>()
            })),
            DataType::Number(num_ty) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => {
                        NumberType::<NUM_TYPE>::from_data(
                            (0..len).map(|_| SmallRng::from_entropy().gen()),
                        )
                    }
                })
            }
            DataType::Decimal(t) => match t {
                DecimalDataType::Decimal128(size) => {
                    let values = (0..len)
                        .map(|_| i128::from(SmallRng::from_entropy().gen::<i16>()))
                        .collect::<Vec<i128>>();
                    Column::Decimal(DecimalColumn::Decimal128(values.into(), *size))
                }
                DecimalDataType::Decimal256(size) => {
                    let values = (0..len)
                        .map(|_| i256::from(SmallRng::from_entropy().gen::<i16>()))
                        .collect::<Vec<i256>>();
                    Column::Decimal(DecimalColumn::Decimal256(values.into(), *size))
                }
            },
            DataType::Timestamp => TimestampType::from_data(
                (0..len)
                    .map(|_| SmallRng::from_entropy().gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX))
                    .collect::<Vec<i64>>(),
            ),
            DataType::Date => DateType::from_data(
                (0..len)
                    .map(|_| SmallRng::from_entropy().gen_range(DATE_MIN..=DATE_MAX))
                    .collect::<Vec<i32>>(),
            ),
            DataType::Nullable(ty) => Column::Nullable(Box::new(NullableColumn {
                column: Column::random(ty, len),
                validity: Bitmap::from(
                    (0..len)
                        .map(|_| SmallRng::from_entropy().gen_bool(0.5))
                        .collect::<Vec<bool>>(),
                ),
            })),
            DataType::Array(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(0);
                for _ in 0..len {
                    inner_len += SmallRng::from_entropy().gen_range(0..=3);
                    offsets.push(inner_len);
                }
                Column::Array(Box::new(ArrayColumn {
                    values: Column::random(inner_ty, inner_len as usize),
                    offsets: offsets.into(),
                }))
            }
            DataType::Map(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(0);
                for _ in 0..len {
                    inner_len += SmallRng::from_entropy().gen_range(0..=3);
                    offsets.push(inner_len);
                }
                Column::Map(Box::new(ArrayColumn {
                    values: Column::random(inner_ty, inner_len as usize),
                    offsets: offsets.into(),
                }))
            }
            DataType::Bitmap => BitmapType::from_data((0..len).map(|_| {
                let data: [u64; 4] = SmallRng::from_entropy().gen();
                let rb = RoaringTreemap::from_iter(data.iter());
                let mut buf = vec![];
                rb.serialize_into(&mut buf)
                    .expect("failed serialize roaring treemap");
                buf
            })),
            DataType::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|ty| Column::random(ty, len))
                    .collect::<Vec<_>>();
                Column::Tuple(fields)
            }
            DataType::Variant => {
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    let val = jsonb::rand_value();
                    data.push(val.to_vec());
                }
                VariantType::from_data(data)
            }
            DataType::Generic(_) => unreachable!(),
        }
    }

    pub fn remove_nullable(&self) -> Self {
        match self {
            Column::Nullable(inner) => inner.column.clone(),
            _ => self.clone(),
        }
    }

    pub fn wrap_nullable(self, validity: Option<Bitmap>) -> Self {
        match self {
            c @ Column::Null { .. } => c,
            Column::Nullable(null_column) => {
                let validity = match validity {
                    Some(v) => &v & (&null_column.validity),
                    None => null_column.validity.clone(),
                };
                Column::Nullable(Box::new(NullableColumn {
                    column: null_column.column.clone(),
                    validity,
                }))
            }
            _ => {
                let validity = validity.unwrap_or_else(|| Bitmap::new_constant(true, self.len()));
                Column::Nullable(Box::new(NullableColumn {
                    column: self.clone(),
                    validity,
                }))
            }
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            Column::Null { .. } => std::mem::size_of::<usize>(),
            Column::EmptyArray { .. } => std::mem::size_of::<usize>(),
            Column::EmptyMap { .. } => std::mem::size_of::<usize>(),
            Column::Number(NumberColumn::UInt8(col)) => col.len(),
            Column::Number(NumberColumn::UInt16(col)) => col.len() * 2,
            Column::Number(NumberColumn::UInt32(col)) => col.len() * 4,
            Column::Number(NumberColumn::UInt64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Float32(col)) => col.len() * 4,
            Column::Number(NumberColumn::Float64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Int8(col)) => col.len(),
            Column::Number(NumberColumn::Int16(col)) => col.len() * 2,
            Column::Number(NumberColumn::Int32(col)) => col.len() * 4,
            Column::Number(NumberColumn::Int64(col)) => col.len() * 8,
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col.len() * 32,
            Column::Boolean(c) => c.as_slice().0.len(),
            Column::String(col) => col.memory_size(),
            Column::Timestamp(col) => col.len() * 8,
            Column::Date(col) => col.len() * 4,
            Column::Array(col) => col.values.memory_size() + col.offsets.len() * 8,
            Column::Map(col) => col.values.memory_size() + col.offsets.len() * 8,
            Column::Bitmap(col) => col.memory_size(),
            Column::Nullable(c) => c.column.memory_size() + c.validity.as_slice().0.len(),
            Column::Tuple(fields) => fields.iter().map(|f| f.memory_size()).sum(),
            Column::Variant(col) => col.memory_size(),
        }
    }

    pub fn serialize_size(&self) -> usize {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => 0,
            Column::Number(NumberColumn::UInt8(col)) => col.len(),
            Column::Number(NumberColumn::UInt16(col)) => col.len() * 2,
            Column::Number(NumberColumn::UInt32(col)) => col.len() * 4,
            Column::Number(NumberColumn::UInt64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Float32(col)) => col.len() * 4,
            Column::Number(NumberColumn::Float64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Int8(col)) => col.len(),
            Column::Number(NumberColumn::Int16(col)) => col.len() * 2,
            Column::Number(NumberColumn::Int32(col)) | Column::Date(col) => col.len() * 4,
            Column::Number(NumberColumn::Int64(col)) | Column::Timestamp(col) => col.len() * 8,
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col.len() * 32,
            Column::Boolean(c) => c.len(),
            Column::String(col) | Column::Bitmap(col) | Column::Variant(col) => col.memory_size(),
            Column::Array(col) | Column::Map(col) => col.values.serialize_size() + col.len() * 8,
            Column::Nullable(c) => c.column.serialize_size() + c.len(),
            Column::Tuple(fields) => fields.iter().map(|f| f.serialize_size()).sum(),
        }
    }

    /// Returns (is_all_null, Option bitmap)
    pub fn validity(&self) -> (bool, Option<&Bitmap>) {
        match self {
            Column::Null { .. } => (true, None),
            Column::Nullable(c) => {
                if c.validity.unset_bits() == c.validity.len() {
                    (true, Some(&c.validity))
                } else {
                    (false, Some(&c.validity))
                }
            }
            _ => (false, None),
        }
    }
}

/// Serialize a column to a base64 string.
/// Because we may use serde::json/bincode to serialize the column, so we wrap it into string
impl Serialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let bytes = serialize_column(self);
        let base64_str = general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D>(deserializer: D) -> Result<Column, D::Error>
    where D: Deserializer<'de> {
        struct ColumnVisitor;

        impl<'de> Visitor<'de> for ColumnVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an arrow chunk with exactly one column")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where E: serde::de::Error {
                let bytes = general_purpose::STANDARD.decode(v).unwrap();
                let column = deserialize_column(&bytes)
                    .expect("expecting an arrow chunk with exactly one column");
                Ok(column)
            }
        }

        deserializer.deserialize_string(ColumnVisitor)
    }
}

impl Eq for Column {}

impl ColumnBuilder {
    pub fn from_column(col: Column) -> Self {
        match col {
            Column::Null { len } => ColumnBuilder::Null { len },
            Column::EmptyArray { len } => ColumnBuilder::EmptyArray { len },
            Column::EmptyMap { len } => ColumnBuilder::EmptyMap { len },
            Column::Number(col) => ColumnBuilder::Number(NumberColumnBuilder::from_column(col)),
            Column::Decimal(col) => ColumnBuilder::Decimal(DecimalColumnBuilder::from_column(col)),
            Column::Boolean(col) => ColumnBuilder::Boolean(bitmap_into_mut(col)),
            Column::String(col) => ColumnBuilder::String(StringColumnBuilder::from_column(col)),
            Column::Timestamp(col) => ColumnBuilder::Timestamp(buffer_into_mut(col)),
            Column::Date(col) => ColumnBuilder::Date(buffer_into_mut(col)),
            Column::Array(box col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Map(box col) => {
                ColumnBuilder::Map(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Bitmap(col) => ColumnBuilder::Bitmap(StringColumnBuilder::from_column(col)),
            Column::Nullable(box col) => {
                ColumnBuilder::Nullable(Box::new(NullableColumnBuilder::from_column(col)))
            }
            Column::Tuple(fields) => ColumnBuilder::Tuple(
                fields
                    .iter()
                    .map(|col| ColumnBuilder::from_column(col.clone()))
                    .collect(),
            ),
            Column::Variant(col) => ColumnBuilder::Variant(StringColumnBuilder::from_column(col)),
        }
    }

    pub fn repeat(scalar: &ScalarRef, n: usize, data_type: &DataType) -> ColumnBuilder {
        if !scalar.is_null() {
            if let DataType::Nullable(ty) = data_type {
                let mut builder = ColumnBuilder::with_capacity(ty, 1);
                for _ in 0..n {
                    builder.push(scalar.clone());
                }
                return ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                    builder,
                    validity: Bitmap::new_constant(true, n).make_mut(),
                }));
            }
        }

        match scalar {
            ScalarRef::Null => match data_type {
                DataType::Null => ColumnBuilder::Null { len: n },
                DataType::Nullable(ty) => {
                    let mut builder = ColumnBuilder::with_capacity(ty, 1);
                    for _ in 0..n {
                        builder.push_default();
                    }
                    ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                        builder,
                        validity: Bitmap::new_constant(false, n).make_mut(),
                    }))
                }
                _ => unreachable!(),
            },
            ScalarRef::EmptyArray => ColumnBuilder::EmptyArray { len: n },
            ScalarRef::EmptyMap => ColumnBuilder::EmptyMap { len: n },
            ScalarRef::Number(num) => ColumnBuilder::Number(NumberColumnBuilder::repeat(*num, n)),
            ScalarRef::Decimal(dec) => {
                ColumnBuilder::Decimal(DecimalColumnBuilder::repeat(*dec, n))
            }
            ScalarRef::Boolean(b) => ColumnBuilder::Boolean(Bitmap::new_constant(*b, n).make_mut()),
            ScalarRef::String(s) => ColumnBuilder::String(StringColumnBuilder::repeat(s, n)),
            ScalarRef::Timestamp(d) => ColumnBuilder::Timestamp(vec![*d; n]),
            ScalarRef::Date(d) => ColumnBuilder::Date(vec![*d; n]),
            ScalarRef::Array(col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::repeat(col, n)))
            }
            ScalarRef::Map(col) => ColumnBuilder::Map(Box::new(ArrayColumnBuilder::repeat(col, n))),
            ScalarRef::Bitmap(b) => {
                let rb =
                    RoaringTreemap::deserialize_from(*b).expect("failed to deserialize bitmap");
                let mut buf = vec![];
                rb.serialize_into(&mut buf)
                    .expect("failed to serialize bitmap");
                ColumnBuilder::Bitmap(StringColumnBuilder::repeat(&buf, n))
            }
            ScalarRef::Tuple(fields) => {
                let fields_ty = match data_type {
                    DataType::Tuple(fields_ty) => fields_ty,
                    _ => unreachable!(),
                };
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .zip(fields_ty)
                        .map(|(field, ty)| ColumnBuilder::repeat(field, n, ty))
                        .collect(),
                )
            }
            ScalarRef::Variant(s) => ColumnBuilder::Variant(StringColumnBuilder::repeat(s, n)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnBuilder::Null { len } => *len,
            ColumnBuilder::EmptyArray { len } => *len,
            ColumnBuilder::EmptyMap { len } => *len,
            ColumnBuilder::Number(col) => col.len(),
            ColumnBuilder::Decimal(col) => col.len(),
            ColumnBuilder::Boolean(builder) => builder.len(),
            ColumnBuilder::String(builder) => builder.len(),
            ColumnBuilder::Timestamp(builder) => builder.len(),
            ColumnBuilder::Date(builder) => builder.len(),
            ColumnBuilder::Array(builder) => builder.len(),
            ColumnBuilder::Map(builder) => builder.len(),
            ColumnBuilder::Bitmap(builder) => builder.len(),
            ColumnBuilder::Nullable(builder) => builder.len(),
            ColumnBuilder::Tuple(fields) => fields[0].len(),
            ColumnBuilder::Variant(builder) => builder.len(),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            ColumnBuilder::Null { .. } => std::mem::size_of::<usize>(),
            ColumnBuilder::EmptyArray { .. } => std::mem::size_of::<usize>(),
            ColumnBuilder::EmptyMap { .. } => std::mem::size_of::<usize>(),
            ColumnBuilder::Number(NumberColumnBuilder::UInt8(builder)) => builder.len(),
            ColumnBuilder::Number(NumberColumnBuilder::UInt16(builder)) => builder.len() * 2,
            ColumnBuilder::Number(NumberColumnBuilder::UInt32(builder)) => builder.len() * 4,
            ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => builder.len() * 8,
            ColumnBuilder::Number(NumberColumnBuilder::Float32(builder)) => builder.len() * 4,
            ColumnBuilder::Number(NumberColumnBuilder::Float64(builder)) => builder.len() * 8,
            ColumnBuilder::Number(NumberColumnBuilder::Int8(builder)) => builder.len(),
            ColumnBuilder::Number(NumberColumnBuilder::Int16(builder)) => builder.len() * 2,
            ColumnBuilder::Number(NumberColumnBuilder::Int32(builder)) => builder.len() * 4,
            ColumnBuilder::Number(NumberColumnBuilder::Int64(builder)) => builder.len() * 8,
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(builder, _)) => {
                builder.len() * 16
            }
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(builder, _)) => {
                builder.len() * 32
            }
            ColumnBuilder::Boolean(c) => c.as_slice().len(),
            ColumnBuilder::String(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::Timestamp(col) => col.len() * 8,
            ColumnBuilder::Date(col) => col.len() * 4,
            ColumnBuilder::Array(col) => col.builder.memory_size() + col.offsets.len() * 8,
            ColumnBuilder::Map(col) => col.builder.memory_size() + col.offsets.len() * 8,
            ColumnBuilder::Bitmap(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::Nullable(c) => c.builder.memory_size() + c.validity.as_slice().len(),
            ColumnBuilder::Tuple(fields) => fields.iter().map(|f| f.memory_size()).sum(),
            ColumnBuilder::Variant(col) => col.data.len() + col.offsets.len() * 8,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnBuilder::Null { .. } => DataType::Null,
            ColumnBuilder::EmptyArray { .. } => DataType::EmptyArray,
            ColumnBuilder::EmptyMap { .. } => DataType::EmptyMap,
            ColumnBuilder::Number(col) => with_number_type!(|NUM_TYPE| match col {
                NumberColumnBuilder::NUM_TYPE(_) => DataType::Number(NumberDataType::NUM_TYPE),
            }),
            ColumnBuilder::Decimal(col) => with_decimal_type!(|DECIMAL_TYPE| match col {
                DecimalColumnBuilder::DECIMAL_TYPE(_, size) =>
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size)),
            }),
            ColumnBuilder::Boolean(_) => DataType::Boolean,
            ColumnBuilder::String(_) => DataType::String,
            ColumnBuilder::Timestamp(_) => DataType::Timestamp,
            ColumnBuilder::Date(_) => DataType::Date,
            ColumnBuilder::Array(col) => {
                let inner = col.builder.data_type();
                DataType::Array(Box::new(inner))
            }
            ColumnBuilder::Map(col) => {
                let inner = col.builder.data_type();
                DataType::Map(Box::new(inner))
            }
            ColumnBuilder::Bitmap(_) => DataType::Bitmap,
            ColumnBuilder::Nullable(col) => DataType::Nullable(Box::new(col.builder.data_type())),
            ColumnBuilder::Tuple(fields) => {
                DataType::Tuple(fields.iter().map(|f| f.data_type()).collect::<Vec<_>>())
            }
            ColumnBuilder::Variant(_) => DataType::Variant,
        }
    }

    pub fn with_capacity(ty: &DataType, capacity: usize) -> ColumnBuilder {
        ColumnBuilder::with_capacity_hint(ty, capacity, true)
    }

    /// Create a new column builder with capacity and enable_datasize_hint
    /// enable_datasize_hint is used in StringColumnBuilder to decide whether to pre-allocate values
    pub fn with_capacity_hint(
        ty: &DataType,
        capacity: usize,
        enable_datasize_hint: bool,
    ) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len: 0 },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len: 0 },
            DataType::EmptyMap => ColumnBuilder::EmptyMap { len: 0 },
            DataType::Number(num_ty) => {
                ColumnBuilder::Number(NumberColumnBuilder::with_capacity(num_ty, capacity))
            }
            DataType::Decimal(decimal_ty) => {
                ColumnBuilder::Decimal(DecimalColumnBuilder::with_capacity(decimal_ty, capacity))
            }
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::with_capacity(capacity)),
            DataType::String => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::String(StringColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Timestamp => ColumnBuilder::Timestamp(Vec::with_capacity(capacity)),
            DataType::Date => ColumnBuilder::Date(Vec::with_capacity(capacity)),
            DataType::Nullable(ty) => ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                builder: Self::with_capacity_hint(ty, capacity, enable_datasize_hint),
                validity: MutableBitmap::with_capacity(capacity),
            })),
            DataType::Array(ty) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder {
                    builder: Self::with_capacity_hint(ty, 0, enable_datasize_hint),
                    offsets,
                }))
            }
            DataType::Map(ty) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::Map(Box::new(ArrayColumnBuilder {
                    builder: Self::with_capacity_hint(ty, 0, enable_datasize_hint),
                    offsets,
                }))
            }
            DataType::Tuple(fields) => {
                assert!(!fields.is_empty());
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .map(|field| {
                            Self::with_capacity_hint(field, capacity, enable_datasize_hint)
                        })
                        .collect(),
                )
            }
            DataType::Bitmap => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Bitmap(StringColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Variant => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Variant(StringColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
        }
    }

    pub fn push(&mut self, item: ScalarRef) {
        match (self, item) {
            (ColumnBuilder::Null { len }, ScalarRef::Null) => *len += 1,
            (ColumnBuilder::EmptyArray { len }, ScalarRef::EmptyArray) => *len += 1,
            (ColumnBuilder::EmptyMap { len }, ScalarRef::EmptyMap) => *len += 1,
            (ColumnBuilder::Number(builder), ScalarRef::Number(value)) => builder.push(value),
            (ColumnBuilder::Decimal(builder), ScalarRef::Decimal(value)) => builder.push(value),
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => builder.push(value),
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                builder.put_slice(value);
                builder.commit_row();
            }
            (ColumnBuilder::Timestamp(builder), ScalarRef::Timestamp(value)) => {
                builder.push(value);
            }
            (ColumnBuilder::Date(builder), ScalarRef::Date(value)) => builder.push(value),
            (ColumnBuilder::Array(builder), ScalarRef::Array(value)) => {
                builder.push(value);
            }
            (ColumnBuilder::Map(builder), ScalarRef::Map(value)) => {
                builder.push(value);
            }
            (ColumnBuilder::Bitmap(builder), ScalarRef::Bitmap(value)) => {
                builder.put_slice(value);
                builder.commit_row();
            }
            (ColumnBuilder::Nullable(builder), ScalarRef::Null) => {
                builder.push_null();
            }
            (ColumnBuilder::Nullable(builder), scalar) => {
                builder.push(scalar);
            }
            (ColumnBuilder::Tuple(fields), ScalarRef::Tuple(value)) => {
                assert_eq!(fields.len(), value.len());
                for (field, scalar) in fields.iter_mut().zip(value.iter()) {
                    field.push(scalar.clone());
                }
            }
            (ColumnBuilder::Variant(builder), ScalarRef::Variant(value)) => {
                builder.put_slice(value);
                builder.commit_row();
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        }
    }

    pub fn push_default(&mut self) {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::EmptyMap { len } => *len += 1,
            ColumnBuilder::Number(builder) => builder.push_default(),
            ColumnBuilder::Decimal(builder) => builder.push_default(),
            ColumnBuilder::Boolean(builder) => builder.push(false),
            ColumnBuilder::String(builder) => builder.commit_row(),
            ColumnBuilder::Timestamp(builder) => builder.push(0),
            ColumnBuilder::Date(builder) => builder.push(0),
            ColumnBuilder::Array(builder) => builder.push_default(),
            ColumnBuilder::Map(builder) => builder.push_default(),
            ColumnBuilder::Bitmap(builder) => builder.commit_row(),
            ColumnBuilder::Nullable(builder) => builder.push_null(),
            ColumnBuilder::Tuple(fields) => {
                for field in fields {
                    field.push_default();
                }
            }
            ColumnBuilder::Variant(builder) => {
                builder.put_slice(JSONB_NULL);
                builder.commit_row();
            }
        }
    }

    pub fn push_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::EmptyMap { len } => *len += 1,
            ColumnBuilder::Number(builder) => with_number_mapped_type!(|NUM_TYPE| match builder {
                NumberColumnBuilder::NUM_TYPE(builder) => {
                    let value: NUM_TYPE = reader.read_scalar()?;
                    builder.push(value);
                }
            }),
            ColumnBuilder::Decimal(builder) => {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match builder {
                    DecimalColumnBuilder::DECIMAL_TYPE(builder, _) =>
                        builder.push(DECIMAL_TYPE::de_binary(reader)),
                })
            }
            ColumnBuilder::Boolean(builder) => {
                let v: bool = reader.read_scalar()?;
                builder.push(v);
            }
            ColumnBuilder::String(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder) => {
                let offset = reader.read_scalar::<u64>()? as usize;
                builder.data.resize(offset + builder.data.len(), 0);
                let last = *builder.offsets.last().unwrap() as usize;
                reader.read_exact(&mut builder.data[last..last + offset])?;
                builder.commit_row();
            }
            ColumnBuilder::Timestamp(builder) => {
                let value: i64 = reader.read_scalar()?;
                check_timestamp(value)?;
                builder.push(value);
            }
            ColumnBuilder::Date(builder) => {
                let value: i32 = reader.read_scalar()?;
                builder.push(value);
            }
            ColumnBuilder::Array(builder) => {
                let len = reader.read_scalar::<u64>()?;
                for _ in 0..len {
                    builder.builder.push_binary(reader)?;
                }
                builder.commit_row();
            }
            ColumnBuilder::Map(builder) => {
                const KEY: usize = 0;
                const VALUE: usize = 1;
                let len = reader.read_scalar::<u64>()?;
                let map_builder = builder.builder.as_tuple_mut().unwrap();
                for _ in 0..len {
                    map_builder[KEY].push_binary(reader)?;
                    map_builder[VALUE].push_binary(reader)?;
                }
                builder.commit_row();
            }
            ColumnBuilder::Nullable(builder) => {
                let valid: bool = reader.read_scalar()?;
                if valid {
                    builder.builder.push_binary(reader)?;
                    builder.validity.push(true);
                } else {
                    builder.push_null();
                }
            }
            ColumnBuilder::Tuple(fields) => {
                for field in fields {
                    field.push_binary(reader)?;
                }
            }
        };

        Ok(())
    }

    pub fn push_fix_len_binaries(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        match self {
            ColumnBuilder::Null { len } => *len += rows,
            ColumnBuilder::EmptyArray { len } => *len += rows,
            ColumnBuilder::EmptyMap { len } => *len += rows,
            ColumnBuilder::Number(builder) => with_number_mapped_type!(|NUM_TYPE| match builder {
                NumberColumnBuilder::NUM_TYPE(builder) => {
                    for row in 0..rows {
                        let mut reader = &reader[step * row..];
                        let value: NUM_TYPE = reader.read_scalar()?;
                        builder.push(value);
                    }
                }
            }),
            ColumnBuilder::Decimal(builder) => {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match builder {
                    DecimalColumnBuilder::DECIMAL_TYPE(builder, _) => {
                        for row in 0..rows {
                            let mut reader = &reader[step * row..];
                            builder.push(DECIMAL_TYPE::de_binary(&mut reader));
                        }
                    }
                })
            }
            ColumnBuilder::Boolean(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let v: bool = reader.read_scalar()?;
                    builder.push(v);
                }
            }
            ColumnBuilder::String(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder) => {
                for row in 0..rows {
                    let reader = &reader[step * row..];
                    builder.put_slice(reader);
                    builder.commit_row();
                }
            }

            ColumnBuilder::Timestamp(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let value: i64 = reader.read_scalar()?;
                    check_timestamp(value)?;
                    builder.push(value);
                }
            }
            ColumnBuilder::Date(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let value: i32 = reader.read_scalar()?;
                    builder.push(value);
                }
            }
            ColumnBuilder::Array(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let len = reader.read_uvarint()?;
                    for _ in 0..len {
                        builder.builder.push_binary(&mut reader)?;
                    }
                    builder.commit_row();
                }
            }
            ColumnBuilder::Map(builder) => {
                const KEY: usize = 0;
                const VALUE: usize = 1;
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let map_builder = builder.builder.as_tuple_mut().unwrap();
                    let len = reader.read_uvarint()?;
                    for _ in 0..len {
                        map_builder[KEY].push_binary(&mut reader)?;
                        map_builder[VALUE].push_binary(&mut reader)?;
                    }
                    builder.commit_row();
                }
            }
            ColumnBuilder::Nullable(_) => {
                unimplemented!()
            }
            ColumnBuilder::Tuple(fields) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    for field in fields.iter_mut() {
                        field.push_binary(&mut reader)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn pop(&mut self) -> Option<Scalar> {
        match self {
            ColumnBuilder::Null { len } => {
                if *len > 0 {
                    *len -= 1;
                    Some(Scalar::Null)
                } else {
                    None
                }
            }
            ColumnBuilder::EmptyArray { len } => {
                if *len > 0 {
                    *len -= 1;
                    Some(Scalar::EmptyArray)
                } else {
                    None
                }
            }
            ColumnBuilder::EmptyMap { len } => {
                if *len > 0 {
                    *len -= 1;
                    Some(Scalar::EmptyMap)
                } else {
                    None
                }
            }
            ColumnBuilder::Number(builder) => builder.pop().map(Scalar::Number),
            ColumnBuilder::Decimal(builder) => builder.pop().map(Scalar::Decimal),
            ColumnBuilder::Boolean(builder) => builder.pop().map(Scalar::Boolean),
            ColumnBuilder::String(builder) => builder.pop().map(Scalar::String),
            ColumnBuilder::Timestamp(builder) => builder.pop().map(Scalar::Timestamp),
            ColumnBuilder::Date(builder) => builder.pop().map(Scalar::Date),
            ColumnBuilder::Array(builder) => builder.pop().map(Scalar::Array),
            ColumnBuilder::Map(builder) => builder.pop().map(Scalar::Map),
            ColumnBuilder::Bitmap(builder) => builder.pop().map(Scalar::Bitmap),
            ColumnBuilder::Nullable(builder) => Some(builder.pop()?.unwrap_or(Scalar::Null)),
            ColumnBuilder::Tuple(fields) => {
                if fields[0].len() > 0 {
                    Some(Scalar::Tuple(
                        fields
                            .iter_mut()
                            .map(|field| field.pop().unwrap())
                            .collect(),
                    ))
                } else {
                    None
                }
            }
            ColumnBuilder::Variant(builder) => builder.pop().map(Scalar::Variant),
        }
    }

    pub fn append_column(&mut self, other: &Column) {
        match (self, other) {
            (ColumnBuilder::Null { len }, Column::Null { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::EmptyArray { len }, Column::EmptyArray { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::EmptyMap { len }, Column::EmptyMap { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::Number(builder), Column::Number(column)) => {
                builder.append_column(column);
            }
            (ColumnBuilder::Decimal(builder), Column::Decimal(column)) => {
                builder.append_column(column);
            }
            (ColumnBuilder::Boolean(builder), Column::Boolean(other)) => {
                append_bitmap(builder, other);
            }
            (ColumnBuilder::String(builder), Column::String(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Variant(builder), Column::Variant(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Timestamp(builder), Column::Timestamp(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::Date(builder), Column::Date(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::Array(builder), Column::Array(other)) => {
                builder.append_column(other.as_ref());
            }
            (ColumnBuilder::Map(builder), Column::Map(other)) => {
                builder.append_column(other.as_ref());
            }
            (ColumnBuilder::Bitmap(builder), Column::Bitmap(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Nullable(builder), Column::Nullable(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Tuple(fields), Column::Tuple(other_fields)) => {
                assert_eq!(fields.len(), other_fields.len());
                for (field, other_field) in fields.iter_mut().zip(other_fields.iter()) {
                    field.append_column(other_field);
                }
            }
            (this, other) => unreachable!(
                "unable append column(data type: {:?}) into builder(data type: {:?})",
                other.data_type(),
                this.data_type()
            ),
        }
    }

    pub fn build(self) -> Column {
        match self {
            ColumnBuilder::Null { len } => Column::Null { len },
            ColumnBuilder::EmptyArray { len } => Column::EmptyArray { len },
            ColumnBuilder::EmptyMap { len } => Column::EmptyMap { len },
            ColumnBuilder::Number(builder) => Column::Number(builder.build()),
            ColumnBuilder::Decimal(builder) => Column::Decimal(builder.build()),
            ColumnBuilder::Boolean(builder) => Column::Boolean(builder.into()),
            ColumnBuilder::String(builder) => Column::String(builder.build()),
            ColumnBuilder::Timestamp(builder) => Column::Timestamp(builder.into()),
            ColumnBuilder::Date(builder) => Column::Date(builder.into()),
            ColumnBuilder::Array(builder) => Column::Array(Box::new(builder.build())),
            ColumnBuilder::Map(builder) => Column::Map(Box::new(builder.build())),
            ColumnBuilder::Bitmap(builder) => Column::Bitmap(builder.build()),
            ColumnBuilder::Nullable(builder) => Column::Nullable(Box::new(builder.build())),
            ColumnBuilder::Tuple(fields) => {
                assert!(fields.iter().map(|field| field.len()).all_equal());
                Column::Tuple(fields.into_iter().map(|field| field.build()).collect())
            }
            ColumnBuilder::Variant(builder) => Column::Variant(builder.build()),
        }
    }

    pub fn build_scalar(self) -> Scalar {
        assert_eq!(self.len(), 1);
        match self {
            ColumnBuilder::Null { .. } => Scalar::Null,
            ColumnBuilder::EmptyArray { .. } => Scalar::EmptyArray,
            ColumnBuilder::EmptyMap { .. } => Scalar::EmptyMap,
            ColumnBuilder::Number(builder) => Scalar::Number(builder.build_scalar()),
            ColumnBuilder::Decimal(builder) => Scalar::Decimal(builder.build_scalar()),
            ColumnBuilder::Boolean(builder) => Scalar::Boolean(builder.get(0)),
            ColumnBuilder::String(builder) => Scalar::String(builder.build_scalar()),
            ColumnBuilder::Timestamp(builder) => Scalar::Timestamp(builder[0]),
            ColumnBuilder::Date(builder) => Scalar::Date(builder[0]),
            ColumnBuilder::Array(builder) => Scalar::Array(builder.build_scalar()),
            ColumnBuilder::Map(builder) => Scalar::Map(builder.build_scalar()),
            ColumnBuilder::Bitmap(builder) => Scalar::Bitmap(builder.build_scalar()),
            ColumnBuilder::Nullable(builder) => builder.build_scalar().unwrap_or(Scalar::Null),
            ColumnBuilder::Tuple(fields) => Scalar::Tuple(
                fields
                    .into_iter()
                    .map(|field| field.build_scalar())
                    .collect(),
            ),
            ColumnBuilder::Variant(builder) => Scalar::Variant(builder.build_scalar()),
        }
    }
}

pub struct ColumnIterator<'a> {
    column: &'a Column,
    index: usize,
    len: usize,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = ScalarRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let item = self.column.index(self.index)?;
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.len - self.index;
        (remain, Some(remain))
    }
}

unsafe impl<'a> TrustedLen for ColumnIterator<'a> {}

#[macro_export]
macro_rules! for_all_number_varints {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8, Int8 },
            { i16, Int16 },
            { i32, Int32 },
            { i64, Int64 },
            { u8, UInt8 },
            { u16, UInt16 },
            { u32, UInt32 },
            { u64, UInt64 },
            { f32, Float32 },
            { f64, Float64 },
            { F32, Float32 },
            { F64, Float64 }
        }
    };
}

macro_rules! impl_scalar_from {
    ([], $( { $S: ident, $TY: ident} ),*) => {
        $(
            impl From<$S> for Scalar {
                fn from(value: $S) -> Self {
                    Scalar::Number(NumberScalar::$TY(value.into()))
                }
            }
        )*
    }
}

for_all_number_varints! {impl_scalar_from}
