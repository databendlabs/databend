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

use std::any::Any;
use std::cmp::Ordering;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;
use std::iter::TrustedLen;
use std::ops::Range;

use base64::engine::general_purpose;
use base64::prelude::*;
use binary::BinaryColumnBuilder;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_base::base::OrderedFloat;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use databend_common_io::HybridBitmap;
use databend_common_io::deserialize_bitmap;
use databend_common_io::prelude::BinaryRead;
use enum_as_inner::EnumAsInner;
use geo::Geometry;
use geo::Point;
use geozero::CoordDimensions;
use geozero::ToWkb;
use itertools::Itertools;
use jsonb::RawJsonb;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::Visitor;
use string::StringColumnBuilder;

use crate::BlockEntry;
use crate::ColumnView;
use crate::bitmap::is_hybrid_encoding;
use crate::property::Domain;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumn;
use crate::types::bitmap::BitmapType;
use crate::types::boolean::BooleanDomain;
use crate::types::date::DATE_MAX;
use crate::types::date::DATE_MIN;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalColumnBuilder;
use crate::types::decimal::DecimalDataType;
use crate::types::decimal::DecimalScalar;
use crate::types::geography::Geography;
use crate::types::geography::GeographyColumn;
use crate::types::geography::GeographyRef;
use crate::types::geometry::GeometryType;
use crate::types::geometry::compare_geometry;
use crate::types::i256;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::nullable::NullableColumnVec;
use crate::types::nullable::NullableDomain;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::number::NumberColumn;
use crate::types::number::NumberColumnBuilder;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::opaque::OpaqueColumn;
use crate::types::opaque::OpaqueColumnBuilder;
use crate::types::opaque::OpaqueColumnVec;
use crate::types::opaque::OpaqueScalar;
use crate::types::opaque::OpaqueScalarRef;
use crate::types::opaque::OpaqueType;
use crate::types::string::StringColumn;
use crate::types::string::StringDomain;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::timestamp::clamp_timestamp;
use crate::types::timestamp_tz::TimestampTzType;
use crate::types::variant::JSONB_NULL;
use crate::types::vector::VectorColumn;
use crate::types::vector::VectorColumnBuilder;
use crate::types::*;
use crate::utils::FromData;
use crate::utils::arrow::append_bitmap;
use crate::utils::arrow::bitmap_into_mut;
use crate::utils::arrow::buffer_into_mut;
use crate::utils::arrow::deserialize_column;
use crate::utils::arrow::serialize_column;
use crate::values::decimal::DecimalColumnVec;
use crate::values::map::KvPair;
use crate::with_decimal_mapped_type;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::with_number_type;
use crate::with_opaque_size;
use crate::with_opaque_size_mapped;
use crate::with_opaque_type;

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Value<T: AccessType> {
    Scalar(T::Scalar),
    Column(T::Column),
}

/// Note:
/// We must modify IndexScalar if we modify Scalar
#[frozen_api("6d4a04cd")]
#[derive(
    Debug,
    Clone,
    EnumAsInner,
    Eq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    FrozenAPI,
)]
pub enum Scalar {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    TimestampTz(timestamp_tz),
    Date(i32),
    Interval(months_days_micros),
    Boolean(bool),
    Binary(Vec<u8>),
    String(String),
    Array(Column),
    Map(Column),
    Bitmap(Vec<u8>),
    Tuple(Vec<Scalar>),
    Variant(Vec<u8>),
    Geometry(Vec<u8>),
    Geography(Geography),
    Vector(VectorScalar),
    Opaque(OpaqueScalar),
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
    Binary(&'a [u8]),
    String(&'a str),
    Timestamp(i64),
    TimestampTz(timestamp_tz),
    Date(i32),
    Interval(months_days_micros),
    Array(Column),
    Map(Column),
    Bitmap(&'a [u8]),
    Tuple(Vec<ScalarRef<'a>>),
    Variant(&'a [u8]),
    Geometry(&'a [u8]),
    Geography(GeographyRef<'a>),
    Vector(VectorScalarRef<'a>),
    Opaque(OpaqueScalarRef<'a>),
}

#[derive(Clone, EnumAsInner)]
pub enum Column {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumn),
    Decimal(DecimalColumn),
    Boolean(Bitmap),
    Binary(BinaryColumn),
    String(StringColumn),
    Timestamp(Buffer<i64>),
    TimestampTz(Buffer<timestamp_tz>),
    Date(Buffer<i32>),
    Interval(Buffer<months_days_micros>),
    Array(Box<ArrayColumn<AnyType>>),
    Map(Box<ArrayColumn<AnyType>>),
    Bitmap(BinaryColumn),
    Nullable(Box<NullableColumn<AnyType>>),
    Tuple(Vec<Column>),
    Variant(BinaryColumn),
    Geometry(BinaryColumn),
    Geography(GeographyColumn),
    Vector(VectorColumn),
    Opaque(OpaqueColumn),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RandomOptions {
    pub seed: Option<u64>,
    pub min_string_len: usize,
    pub max_string_len: usize,
    pub max_array_len: usize,
}

impl Default for RandomOptions {
    fn default() -> Self {
        RandomOptions {
            seed: None,
            min_string_len: 0,
            max_string_len: 5,
            max_array_len: 3,
        }
    }
}

#[derive(Clone, EnumAsInner, Debug, PartialEq)]
pub enum ColumnVec {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberColumnVec),
    Decimal(DecimalColumnVec),
    Boolean(Vec<Bitmap>),
    Binary(Vec<BinaryColumn>),
    String(Vec<StringColumn>),
    Timestamp(Vec<Buffer<i64>>),
    TimestampTz(Vec<Buffer<timestamp_tz>>),
    Date(Vec<Buffer<i32>>),
    Interval(Vec<Buffer<months_days_micros>>),
    Array(Vec<ArrayColumn<AnyType>>),
    Map(Vec<ArrayColumn<KvPair<AnyType, AnyType>>>),
    Bitmap(Vec<BinaryColumn>),
    Nullable(Box<NullableColumnVec>),
    Tuple(Vec<ColumnVec>),
    Variant(Vec<BinaryColumn>),
    Geometry(Vec<BinaryColumn>),
    Geography(Vec<GeographyColumn>),
    Vector(VectorColumnVec),
    Opaque(OpaqueColumnVec),
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum ColumnBuilder {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumnBuilder),
    Decimal(DecimalColumnBuilder),
    Boolean(MutableBitmap),
    Binary(BinaryColumnBuilder),
    String(StringColumnBuilder),
    Timestamp(Vec<i64>),
    TimestampTz(Vec<timestamp_tz>),
    Date(Vec<i32>),
    Interval(Vec<months_days_micros>),
    Array(Box<ArrayColumnBuilder<AnyType>>),
    Map(Box<ArrayColumnBuilder<AnyType>>),
    Bitmap(BinaryColumnBuilder),
    Nullable(Box<NullableColumnBuilder<AnyType>>),
    Tuple(Vec<ColumnBuilder>),
    Variant(BinaryColumnBuilder),
    Geometry(BinaryColumnBuilder),
    Geography(BinaryColumnBuilder),
    Vector(VectorColumnBuilder),
    Opaque(OpaqueColumnBuilder),
}

impl<T: AccessType> Value<T> {
    pub fn semantically_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Scalar(s1), Value::Scalar(s2)) => s1 == s2,
            (Value::Column(c1), Value::Column(c2)) => c1 == c2,
            (Value::Scalar(s), Value::Column(c)) | (Value::Column(c), Value::Scalar(s)) => {
                let s = T::to_scalar_ref(s);
                T::iter_column(c).all(|scalar| scalar == s)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Value::Scalar(_) => 1,
            Value::Column(col) => T::column_len(col),
        }
    }

    pub fn memory_size(&self, gc: bool) -> usize {
        match self {
            Value::Scalar(scalar) => T::scalar_memory_size(&T::to_scalar_ref(scalar)),
            Value::Column(c) => T::column_memory_size(c, gc),
        }
    }

    pub fn index(&self, index: usize) -> Option<T::ScalarRef<'_>> {
        match self {
            Value::Scalar(scalar) => Some(T::to_scalar_ref(scalar)),
            Value::Column(col) => T::index_column(col, index),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> T::ScalarRef<'_> {
        unsafe {
            match self {
                Value::Scalar(scalar) => T::to_scalar_ref(scalar),
                Value::Column(c) => T::index_column_unchecked(c, index),
            }
        }
    }
}

impl<T: ValueType> Value<T> {
    pub fn upcast_with_type(self, data_type: &DataType) -> Value<AnyType> {
        match self {
            Value::Scalar(scalar) => Value::Scalar(T::upcast_scalar_with_type(scalar, data_type)),
            Value::Column(col) => Value::Column(T::upcast_column_with_type(col, data_type)),
        }
    }
}

impl<T: ArgType> Value<T> {
    pub fn upcast(self) -> Value<AnyType> {
        let data_type = T::data_type();
        match self {
            Value::Scalar(scalar) => Value::Scalar(T::upcast_scalar_with_type(scalar, &data_type)),
            Value::Column(col) => Value::Column(T::upcast_column_with_type(col, &data_type)),
        }
    }
}

impl<T: AccessType> Value<NullableType<T>> {
    pub fn validity(&self, num_rows: usize) -> Bitmap {
        match self {
            Value::Scalar(None) => Bitmap::new_zeroed(num_rows),
            Value::Scalar(Some(_)) => Bitmap::new_trued(num_rows),
            Value::Column(col) => col.validity.clone(),
        }
    }

    pub fn value(&self) -> Option<Value<T>> {
        match self {
            Value::Scalar(None) => None,
            Value::Scalar(Some(s)) => Some(Value::Scalar(s.clone())),
            Value::Column(col) => Some(Value::Column(col.column.clone())),
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

    pub fn into_full_column(self, ty: &DataType, num_rows: usize) -> Column {
        match self {
            Value::Scalar(s) => {
                let builder = ColumnBuilder::repeat(&s.as_ref(), num_rows, ty);
                builder.build()
            }
            Value::Column(c) => c,
        }
    }

    pub fn try_downcast<T: AccessType>(&self) -> Result<Value<T>> {
        match self {
            Value::Scalar(scalar) => Ok(Value::Scalar(T::to_owned_scalar(T::try_downcast_scalar(
                &scalar.as_ref(),
            )?))),
            Value::Column(col) => Ok(Value::Column(T::try_downcast_column(col)?)),
        }
    }

    pub fn wrap_nullable(self, validity: Option<Bitmap>) -> Self {
        match self {
            Value::Column(c) => Value::Column(c.wrap_nullable(validity)),
            scalar => scalar,
        }
    }

    // returns result without nullable and has_null flag
    pub fn remove_nullable(self) -> (Self, bool) {
        match self {
            Value::Scalar(Scalar::Null) => (Value::Scalar(Scalar::Null), true),
            Value::Column(Column::Nullable(box nullable_column)) => (
                Value::Column(nullable_column.column),
                nullable_column.validity.null_count() > 0,
            ),
            other => (other, false),
        }
    }

    pub fn domain(&self, data_type: &DataType) -> Domain {
        match self {
            Value::Scalar(scalar) => scalar.as_ref().domain(data_type),
            Value::Column(col) => col.domain(),
        }
    }

    pub fn is_scalar_null(&self) -> bool {
        *self == Value::Scalar(Scalar::Null)
    }

    pub fn is_value_of_type(&self, data_type: &DataType) -> bool {
        match self {
            Value::Scalar(scalar) => scalar.as_ref().is_value_of_type(data_type),
            Value::Column(column) => column.data_type() == *data_type,
        }
    }
}

impl Scalar {
    pub fn as_ref(&self) -> ScalarRef<'_> {
        match self {
            Scalar::Null => ScalarRef::Null,
            Scalar::EmptyArray => ScalarRef::EmptyArray,
            Scalar::EmptyMap => ScalarRef::EmptyMap,
            Scalar::Number(n) => ScalarRef::Number(*n),
            Scalar::Decimal(d) => ScalarRef::Decimal(*d),
            Scalar::Boolean(b) => ScalarRef::Boolean(*b),
            Scalar::Binary(s) => ScalarRef::Binary(s.as_slice()),
            Scalar::String(s) => ScalarRef::String(s.as_str()),
            Scalar::Timestamp(t) => ScalarRef::Timestamp(*t),
            Scalar::TimestampTz(t) => ScalarRef::TimestampTz(*t),
            Scalar::Date(d) => ScalarRef::Date(*d),
            Scalar::Interval(d) => ScalarRef::Interval(*d),
            Scalar::Array(col) => ScalarRef::Array(col.clone()),
            Scalar::Map(col) => ScalarRef::Map(col.clone()),
            Scalar::Bitmap(b) => ScalarRef::Bitmap(b.as_slice()),
            Scalar::Tuple(fields) => ScalarRef::Tuple(fields.iter().map(Scalar::as_ref).collect()),
            Scalar::Variant(s) => ScalarRef::Variant(s.as_slice()),
            Scalar::Geometry(s) => ScalarRef::Geometry(s.as_slice()),
            Scalar::Geography(g) => ScalarRef::Geography(g.as_ref()),
            Scalar::Vector(v) => ScalarRef::Vector(v.as_ref()),
            Scalar::Opaque(o) => ScalarRef::Opaque(o.as_ref()),
        }
    }

    pub fn default_value(ty: &DataType) -> Scalar {
        match ty {
            DataType::Null => Scalar::Null,
            DataType::EmptyArray => Scalar::EmptyArray,
            DataType::EmptyMap => Scalar::EmptyMap,
            DataType::Boolean => Scalar::Boolean(false),
            DataType::Binary => Scalar::Binary(Vec::new()),
            DataType::String => Scalar::String(String::new()),
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
            DataType::Decimal(size) => {
                with_decimal_type!(
                    |DECIMAL_TYPE| match DecimalDataType::from(*size).data_kind() {
                        DecimalDataKind::DECIMAL_TYPE => {
                            Scalar::Decimal(DecimalDataType::DECIMAL_TYPE(*size).default_scalar())
                        }
                    }
                )
            }
            DataType::Timestamp => Scalar::Timestamp(0),
            DataType::TimestampTz => Scalar::TimestampTz(timestamp_tz::new(0, 0)),
            DataType::Date => Scalar::Date(0),
            DataType::Interval => Scalar::Interval(months_days_micros(0)),
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
            DataType::Geometry => Scalar::Geometry(vec![]),
            DataType::Geography => Scalar::Geography(Geography::default()),
            DataType::Vector(ty) => Scalar::Vector(ty.default_value()),
            _ => unimplemented!(),
        }
    }

    pub fn is_nested_scalar(&self) -> bool {
        match self {
            Scalar::Null
            | Scalar::EmptyArray
            | Scalar::EmptyMap
            | Scalar::Number(_)
            | Scalar::Decimal(_)
            | Scalar::Timestamp(_)
            | Scalar::TimestampTz(_)
            | Scalar::Date(_)
            | Scalar::Interval(_)
            | Scalar::Boolean(_)
            | Scalar::Binary(_)
            | Scalar::String(_)
            | Scalar::Bitmap(_)
            | Scalar::Variant(_)
            | Scalar::Geometry(_)
            | Scalar::Geography(_) => false,
            Scalar::Array(_)
            | Scalar::Map(_)
            | Scalar::Tuple(_)
            | Scalar::Vector(_)
            | Scalar::Opaque(_) => true,
        }
    }

    pub fn is_positive(&self) -> bool {
        match self {
            Scalar::Number(n) => n.is_positive(),
            Scalar::Decimal(d) => d.is_positive(),
            Scalar::Timestamp(t) => *t > 0,
            Scalar::Date(d) => *d > 0,
            Scalar::Interval(i) => i.0.is_positive(),
            _ => unreachable!("is_positive() called on non-numeric scalar"),
        }
    }

    pub fn get_i64(&self) -> Option<i64> {
        match self {
            Scalar::Number(n) => match n {
                NumberScalar::Int8(x) => Some(*x as _),
                NumberScalar::Int16(x) => Some(*x as _),
                NumberScalar::Int32(x) => Some(*x as _),
                NumberScalar::Int64(x) => Some(*x as _),
                NumberScalar::UInt8(x) => Some(*x as _),
                NumberScalar::UInt16(x) => Some(*x as _),
                NumberScalar::UInt32(x) => Some(*x as _),
                NumberScalar::UInt64(x) => i64::try_from(*x).ok(),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Scalar::String(val) => Some(val.as_bytes()),
            Scalar::Geography(val) => Some(val.0.as_slice()),
            Scalar::Binary(val)
            | Scalar::Bitmap(val)
            | Scalar::Variant(val)
            | Scalar::Geometry(val) => Some(val.as_slice()),
            _ => None,
        }
    }
}

impl ScalarRef<'_> {
    pub fn to_owned(&self) -> Scalar {
        match self {
            ScalarRef::Null => Scalar::Null,
            ScalarRef::EmptyArray => Scalar::EmptyArray,
            ScalarRef::EmptyMap => Scalar::EmptyMap,
            ScalarRef::Number(n) => Scalar::Number(*n),
            ScalarRef::Decimal(d) => Scalar::Decimal(*d),
            ScalarRef::Boolean(b) => Scalar::Boolean(*b),
            ScalarRef::Binary(s) => Scalar::Binary(s.to_vec()),
            ScalarRef::String(s) => Scalar::String(s.to_string()),
            ScalarRef::Timestamp(t) => Scalar::Timestamp(*t),
            ScalarRef::TimestampTz(t) => Scalar::TimestampTz(*t),
            ScalarRef::Date(d) => Scalar::Date(*d),
            ScalarRef::Interval(i) => Scalar::Interval(*i),
            ScalarRef::Array(col) => Scalar::Array(col.clone()),
            ScalarRef::Map(col) => Scalar::Map(col.clone()),
            ScalarRef::Bitmap(b) => Scalar::Bitmap(b.to_vec()),
            ScalarRef::Tuple(fields) => {
                Scalar::Tuple(fields.iter().map(ScalarRef::to_owned).collect())
            }
            ScalarRef::Variant(s) => Scalar::Variant(s.to_vec()),
            ScalarRef::Geometry(s) => Scalar::Geometry(s.to_vec()),
            ScalarRef::Geography(s) => Scalar::Geography(s.to_owned()),
            ScalarRef::Vector(s) => Scalar::Vector(s.to_owned()),
            ScalarRef::Opaque(o) => Scalar::Opaque(o.to_owned()),
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
                min: s.to_string(),
                max: Some(s.to_string()),
            }),
            ScalarRef::Timestamp(t) => Domain::Timestamp(SimpleDomain { min: *t, max: *t }),
            ScalarRef::TimestampTz(t) => Domain::TimestampTz(SimpleDomain { min: *t, max: *t }),
            ScalarRef::Date(d) => Domain::Date(SimpleDomain { min: *d, max: *d }),
            ScalarRef::Interval(i) => Domain::Interval(SimpleDomain { min: *i, max: *i }),
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
                    Domain::Map(Some(Box::new(map.domain())))
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
            ScalarRef::Binary(_)
            | ScalarRef::Bitmap(_)
            | ScalarRef::Variant(_)
            | ScalarRef::Geometry(_)
            | ScalarRef::Geography(_)
            | ScalarRef::Vector(_)
            | ScalarRef::Opaque(_) => Domain::Undefined,
        }
    }

    pub fn get_i64(&self) -> Option<i64> {
        match self {
            ScalarRef::Number(n) => match n {
                NumberScalar::Int8(x) => Some(*x as _),
                NumberScalar::Int16(x) => Some(*x as _),
                NumberScalar::Int32(x) => Some(*x as _),
                NumberScalar::Int64(x) => Some(*x as _),
                NumberScalar::UInt8(x) => Some(*x as _),
                NumberScalar::UInt16(x) => Some(*x as _),
                NumberScalar::UInt32(x) => Some(*x as _),
                NumberScalar::UInt64(x) => i64::try_from(*x).ok(),
                _ => None,
            },
            _ => None,
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
            ScalarRef::Decimal(DecimalScalar::Decimal64(_, _)) => 8,
            ScalarRef::Decimal(DecimalScalar::Decimal128(_, _)) => 16,
            ScalarRef::Decimal(DecimalScalar::Decimal256(_, _)) => 32,
            ScalarRef::Boolean(_) => 1,
            ScalarRef::Binary(s) => s.len(),
            ScalarRef::String(s) => s.len(),
            ScalarRef::Timestamp(_) => 8,
            ScalarRef::TimestampTz(_) => 16,
            ScalarRef::Date(_) => 4,
            ScalarRef::Interval(_) => 16,
            ScalarRef::Array(col) => col.memory_size(false),
            ScalarRef::Map(col) => col.memory_size(false),
            ScalarRef::Bitmap(b) => b.len(),
            ScalarRef::Tuple(scalars) => scalars.iter().map(|s| s.memory_size()).sum(),
            ScalarRef::Variant(buf) => buf.len(),
            ScalarRef::Geometry(buf) => buf.len(),
            ScalarRef::Geography(s) => s.0.len(),
            ScalarRef::Vector(s) => s.memory_size(),
            ScalarRef::Opaque(o) => o.memory_size(),
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
                DecimalScalar::DECIMAL_TYPE(_, size) => DataType::Decimal(*size),
            }),
            ScalarRef::Boolean(_) => DataType::Boolean,
            ScalarRef::Binary(_) => DataType::Binary,
            ScalarRef::String(_) => DataType::String,
            ScalarRef::Timestamp(_) => DataType::Timestamp,
            ScalarRef::TimestampTz(_) => DataType::TimestampTz,
            ScalarRef::Date(_) => DataType::Date,
            ScalarRef::Interval(_) => DataType::Interval,
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
            ScalarRef::Geometry(_) => DataType::Geometry,
            ScalarRef::Geography(_) => DataType::Geography,
            ScalarRef::Vector(v) => DataType::Vector(v.data_type()),
            ScalarRef::Opaque(o) => o.data_type(),
        }
    }

    /// Infer the common data type of the scalar and the other scalar.
    ///
    /// If both of the scalar is Null, the data type is `DataType::Null`,
    /// and if one of the scalar is Null, the data type is nullable,
    /// otherwise, the inferred data type is non-nullable.
    ///
    /// Auto type cast is not considered in this method. For example,
    /// `infer_common_type` for a scalar of `UInt8` and a scalar of`UInt16`
    ///  will return `None`.
    pub fn infer_common_type(&self, other: &Self) -> Option<DataType> {
        match (self, other) {
            (ScalarRef::Null, ScalarRef::Null) => Some(DataType::Null),
            (ScalarRef::Null, other) | (other, ScalarRef::Null) => {
                Some(other.infer_data_type().wrap_nullable())
            }
            (ScalarRef::EmptyArray, ScalarRef::EmptyArray) => Some(DataType::EmptyArray),
            (ScalarRef::EmptyMap, ScalarRef::EmptyMap) => Some(DataType::EmptyMap),
            (ScalarRef::Number(s1), ScalarRef::Number(s2)) => {
                with_number_type!(|NUM_TYPE| match (s1, s2) {
                    (NumberScalar::NUM_TYPE(_), NumberScalar::NUM_TYPE(_)) => {
                        Some(DataType::Number(NumberDataType::NUM_TYPE))
                    }
                    _ => None,
                })
            }
            (ScalarRef::Decimal(s1), ScalarRef::Decimal(s2)) => {
                with_decimal_type!(|DECIMAL_TYPE| match (s1, s2) {
                    (
                        DecimalScalar::DECIMAL_TYPE(_, size1),
                        DecimalScalar::DECIMAL_TYPE(_, size2),
                    ) => {
                        if size1 == size2 {
                            Some(DataType::Decimal(*size1))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            }
            (ScalarRef::Boolean(_), ScalarRef::Boolean(_)) => Some(DataType::Boolean),
            (ScalarRef::Binary(_), ScalarRef::Binary(_)) => Some(DataType::Binary),
            (ScalarRef::String(_), ScalarRef::String(_)) => Some(DataType::String),
            (ScalarRef::Timestamp(_), ScalarRef::Timestamp(_)) => Some(DataType::Timestamp),
            (ScalarRef::TimestampTz(_), ScalarRef::TimestampTz(_)) => Some(DataType::TimestampTz),
            (ScalarRef::Date(_), ScalarRef::Date(_)) => Some(DataType::Date),
            (ScalarRef::Array(s1), ScalarRef::Array(s2)) if s1.data_type() == s2.data_type() => {
                Some(DataType::Array(Box::new(s1.data_type())))
            }
            (ScalarRef::Map(s1), ScalarRef::Map(s2)) if s1.data_type() == s2.data_type() => {
                Some(DataType::Map(Box::new(s1.data_type())))
            }
            (ScalarRef::Bitmap(_), ScalarRef::Bitmap(_)) => Some(DataType::Bitmap),
            (ScalarRef::Tuple(s1), ScalarRef::Tuple(s2)) => {
                let inner = s1
                    .iter()
                    .zip(s2.iter())
                    .map(|(s1, s2)| s1.infer_common_type(s2))
                    .collect::<Option<Vec<_>>>()?;
                Some(DataType::Tuple(inner))
            }
            (ScalarRef::Variant(_), ScalarRef::Variant(_)) => Some(DataType::Variant),
            (ScalarRef::Geometry(_), ScalarRef::Geometry(_)) => Some(DataType::Geometry),
            (ScalarRef::Geography(_), ScalarRef::Geography(_)) => Some(DataType::Geography),
            (ScalarRef::Interval(_), ScalarRef::Interval(_)) => Some(DataType::Interval),
            (ScalarRef::Vector(v1), ScalarRef::Vector(v2)) if v1.data_type() == v2.data_type() => {
                Some(DataType::Vector(v1.data_type()))
            }
            _ => None,
        }
    }

    /// Check if the scalar is valid for the given data type.
    pub fn is_value_of_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (ScalarRef::Null, DataType::Null) => true,
            (ScalarRef::Null, DataType::Nullable(_)) => true,
            _ => match (self, data_type.remove_nullable()) {
                (ScalarRef::EmptyArray, DataType::EmptyArray) => true,
                (ScalarRef::EmptyMap, DataType::EmptyMap) => true,
                (ScalarRef::Number(_), DataType::Number(_)) => true,
                (ScalarRef::Decimal(_), DataType::Decimal(_)) => true,
                (ScalarRef::Boolean(_), DataType::Boolean) => true,
                (ScalarRef::Binary(_), DataType::Binary) => true,
                (ScalarRef::String(_), DataType::String) => true,
                (ScalarRef::Timestamp(_), DataType::Timestamp) => true,
                (ScalarRef::TimestampTz(_), DataType::TimestampTz) => true,
                (ScalarRef::Interval(_), DataType::Interval) => true,
                (ScalarRef::Date(_), DataType::Date) => true,
                (ScalarRef::Bitmap(_), DataType::Bitmap) => true,
                (ScalarRef::Variant(_), DataType::Variant) => true,
                (ScalarRef::Geometry(_), DataType::Geometry) => true,
                (ScalarRef::Geography(_), DataType::Geography) => true,
                (ScalarRef::Array(val), DataType::Array(ty)) => val.data_type() == *ty,
                (ScalarRef::Map(val), DataType::Map(ty)) => val.data_type() == *ty,
                (ScalarRef::Tuple(val), DataType::Tuple(ty)) => {
                    if val.len() != ty.len() {
                        return false;
                    }
                    val.iter()
                        .zip(ty)
                        .all(|(val, ty)| val.is_value_of_type(&ty))
                }
                (ScalarRef::Vector(val), DataType::Vector(ty)) => val.data_type() == ty,
                (ScalarRef::Opaque(val), ty) => val.data_type() == ty,
                _ => false,
            },
        }
    }

    /// Estimates the memory size of a scalar value if it were repeated `n` times,
    /// without actually converting it into a column. This avoids unnecessary allocations
    /// and provides a direct calculation based on the scalar type and its associated DataType.
    ///
    /// # Parameters:
    /// - `scalar`: The scalar value to estimate memory size for.
    /// - `n`: The number of times the scalar is hypothetically repeated.
    /// - `data_type`: The data type of the scalar, used for correct size calculations.
    ///
    /// # Returns:
    /// The estimated memory size (in bytes) that `n` repetitions of `scalar` would occupy.
    pub fn estimated_scalar_repeat_size(&self, n: usize, data_type: &DataType) -> usize {
        if let DataType::Nullable(ty) = data_type {
            let mut memory_size = n.div_ceil(8);
            if !self.is_null() {
                memory_size += self.estimated_scalar_repeat_size(n, ty);
            }
            return memory_size;
        }

        match self {
            ScalarRef::Null => std::mem::size_of::<usize>(),
            ScalarRef::EmptyArray | ScalarRef::EmptyMap => std::mem::size_of::<usize>(),
            ScalarRef::Number(_) => n * self.memory_size(),
            ScalarRef::Decimal(_) => n * self.memory_size(),
            ScalarRef::Boolean(_) => n.div_ceil(8),
            ScalarRef::Binary(s) => s.len() * n + (n + 1) * 8,
            ScalarRef::String(s) => n * 16 + if s.len() > 12 && n > 0 { s.len() } else { 0 },
            ScalarRef::Timestamp(_) => n * 8,
            ScalarRef::TimestampTz(_) => n * 16,
            ScalarRef::Date(_) => n * 4,
            ScalarRef::Interval(_) => n * 16,
            ScalarRef::Array(col) => col.memory_size(false) * n + (n + 1) * 8,
            ScalarRef::Map(col) => col.memory_size(false) * n + (n + 1) * 8,
            ScalarRef::Bitmap(b) => b.len() * n + (n + 1) * 8,
            ScalarRef::Tuple(fields) => {
                let DataType::Tuple(fields_ty) = data_type else {
                    unreachable!()
                };
                fields
                    .iter()
                    .zip(fields_ty.iter())
                    .map(|(v, ty)| v.estimated_scalar_repeat_size(n, ty))
                    .sum()
            }
            ScalarRef::Variant(s) => s.len() * n + (n + 1) * 8,
            ScalarRef::Geometry(s) => s.len() * n + (n + 1) * 8,
            ScalarRef::Geography(s) => s.0.len() * n + (n + 1) * 8,
            ScalarRef::Vector(s) => s.memory_size() * n,
            ScalarRef::Opaque(o) => o.memory_size() * n,
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
            (Scalar::Binary(s1), Scalar::Binary(s2)) => s1.partial_cmp(s2),
            (Scalar::String(s1), Scalar::String(s2)) => s1.partial_cmp(s2),
            (Scalar::Timestamp(t1), Scalar::Timestamp(t2)) => t1.partial_cmp(t2),
            (Scalar::TimestampTz(t1), Scalar::TimestampTz(t2)) => t1.partial_cmp(t2),
            (Scalar::Date(d1), Scalar::Date(d2)) => d1.partial_cmp(d2),
            (Scalar::Interval(i1), Scalar::Interval(i2)) => i1.partial_cmp(i2),
            (Scalar::Array(a1), Scalar::Array(a2)) => a1.partial_cmp(a2),
            (Scalar::Map(m1), Scalar::Map(m2)) => m1.partial_cmp(m2),
            (Scalar::Bitmap(b1), Scalar::Bitmap(b2)) => {
                // Bitmap only allows PartialEq
                if is_hybrid_encoding(b1) == is_hybrid_encoding(b2) && b1 == b2 {
                    return Some(Ordering::Equal);
                }
                let Ok(map_1) = deserialize_bitmap(b1) else {
                    return None;
                };
                let Ok(map_2) = deserialize_bitmap(b2) else {
                    return None;
                };
                map_1.eq(&map_2).then_some(Ordering::Equal)
            }
            (Scalar::Tuple(t1), Scalar::Tuple(t2)) => t1.partial_cmp(t2),
            (Scalar::Variant(v1), Scalar::Variant(v2)) => {
                let left_jsonb = RawJsonb::new(v1);
                let right_jsonb = RawJsonb::new(v2);
                left_jsonb.partial_cmp(&right_jsonb)
            }
            (Scalar::Geometry(g1), Scalar::Geometry(g2)) => compare_geometry(g1, g2),
            (Scalar::Geography(g1), Scalar::Geography(g2)) => g1.partial_cmp(g2),
            (Scalar::Vector(v1), Scalar::Vector(v2)) => v1.partial_cmp(v2),
            (Scalar::Opaque(o1), Scalar::Opaque(o2)) => {
                with_opaque_type!(|T| match (o1, o2) {
                    (OpaqueScalar::T(v1), OpaqueScalar::T(v2)) => v1.partial_cmp(v2),
                    _ => None,
                })
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

impl<'b> PartialOrd<ScalarRef<'b>> for ScalarRef<'_> {
    fn partial_cmp(&self, other: &ScalarRef<'b>) -> Option<Ordering> {
        match (self, other) {
            (ScalarRef::Null, ScalarRef::Null) => Some(Ordering::Equal),
            (ScalarRef::EmptyArray, ScalarRef::EmptyArray) => Some(Ordering::Equal),
            (ScalarRef::EmptyMap, ScalarRef::EmptyMap) => Some(Ordering::Equal),
            (ScalarRef::Number(n1), ScalarRef::Number(n2)) => n1.partial_cmp(n2),
            (ScalarRef::Decimal(d1), ScalarRef::Decimal(d2)) => d1.partial_cmp(d2),
            (ScalarRef::Boolean(b1), ScalarRef::Boolean(b2)) => b1.partial_cmp(b2),
            (ScalarRef::Binary(s1), ScalarRef::Binary(s2)) => s1.partial_cmp(s2),
            (ScalarRef::String(s1), ScalarRef::String(s2)) => s1.partial_cmp(s2),
            (ScalarRef::Timestamp(t1), ScalarRef::Timestamp(t2)) => t1.partial_cmp(t2),
            (ScalarRef::TimestampTz(t1), ScalarRef::TimestampTz(t2)) => t1.partial_cmp(t2),
            (ScalarRef::Date(d1), ScalarRef::Date(d2)) => d1.partial_cmp(d2),
            (ScalarRef::Array(a1), ScalarRef::Array(a2)) => a1.partial_cmp(a2),
            (ScalarRef::Map(m1), ScalarRef::Map(m2)) => m1.partial_cmp(m2),
            (ScalarRef::Bitmap(b1), ScalarRef::Bitmap(b2)) => b1.partial_cmp(b2),
            (ScalarRef::Tuple(t1), ScalarRef::Tuple(t2)) => t1.partial_cmp(t2),
            (ScalarRef::Variant(v1), ScalarRef::Variant(v2)) => {
                let left_jsonb = RawJsonb::new(v1);
                let right_jsonb = RawJsonb::new(v2);
                left_jsonb.partial_cmp(&right_jsonb)
            }
            (ScalarRef::Geometry(g1), ScalarRef::Geometry(g2)) => compare_geometry(g1, g2),
            (ScalarRef::Geography(g1), ScalarRef::Geography(g2)) => g1.partial_cmp(g2),
            (ScalarRef::Interval(i1), ScalarRef::Interval(i2)) => i1.partial_cmp(i2),
            (ScalarRef::Vector(v1), ScalarRef::Vector(v2)) => v1.partial_cmp(v2),
            (ScalarRef::Opaque(o1), ScalarRef::Opaque(o2)) => {
                with_opaque_type!(|T| match (o1, o2) {
                    (OpaqueScalarRef::T(v1), OpaqueScalarRef::T(v2)) => v1.partial_cmp(v2),
                    _ => None,
                })
            }

            // By default, null is biggest in pgsql
            (ScalarRef::Null, _) => Some(Ordering::Greater),
            (_, ScalarRef::Null) => Some(Ordering::Less),
            _ => None,
        }
    }
}

impl Ord for ScalarRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl<'b> PartialEq<ScalarRef<'b>> for ScalarRef<'_> {
    fn eq(&self, other: &ScalarRef<'b>) -> bool {
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
            ScalarRef::Binary(v) => v.hash(state),
            ScalarRef::String(v) => v.hash(state),
            ScalarRef::Timestamp(v) => v.hash(state),
            ScalarRef::TimestampTz(v) => v.hash(state),
            ScalarRef::Date(v) => v.hash(state),
            ScalarRef::Interval(v) => v.0.hash(state),
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
            ScalarRef::Geometry(v) => v.hash(state),
            ScalarRef::Geography(v) => v.hash(state),
            ScalarRef::Vector(v) => v.hash(state),
            ScalarRef::Opaque(v) => v.hash(state),
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
            (Column::Binary(col1), Column::Binary(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::String(col1), Column::String(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Timestamp(col1), Column::Timestamp(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::TimestampTz(col1), Column::TimestampTz(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Date(col1), Column::Date(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Interval(col1), Column::Interval(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Array(col1), Column::Array(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Map(col1), Column::Map(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Bitmap(col1), Column::Bitmap(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Nullable(col1), Column::Nullable(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Tuple(fields1), Column::Tuple(fields2)) => fields1.partial_cmp(fields2),
            (Column::Variant(col1), Column::Variant(col2)) => {
                col1.iter().partial_cmp_by(col2.iter(), |v1, v2| {
                    let left_jsonb = RawJsonb::new(v1);
                    let right_jsonb = RawJsonb::new(v2);
                    left_jsonb.partial_cmp(&right_jsonb)
                })
            }
            (Column::Geometry(col1), Column::Geometry(col2)) => {
                col1.iter().partial_cmp_by(col2.iter(), compare_geometry)
            }
            (Column::Geography(col1), Column::Geography(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Vector(col1), Column::Vector(col2)) => col1.partial_cmp(col2),
            (a, b) => {
                if a.len() != b.len() {
                    a.len().partial_cmp(&b.len())
                } else {
                    for (l, r) in AnyType::iter_column(a).zip(AnyType::iter_column(b)) {
                        match l.partial_cmp(&r) {
                            Some(Ordering::Equal) => {}
                            other => return other,
                        }
                    }
                    Some(Ordering::Equal)
                }
            }
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Null { len } => *len,
            Column::EmptyArray { len } => *len,
            Column::EmptyMap { len } => *len,
            Column::Number(col) => col.len(),
            Column::Decimal(col) => col.len(),
            Column::Boolean(col) => col.len(),
            Column::Binary(col) => col.len(),
            Column::String(col) => col.len(),
            Column::Timestamp(col) => col.len(),
            Column::TimestampTz(col) => col.len(),
            Column::Date(col) => col.len(),
            Column::Interval(col) => col.len(),
            Column::Array(col) => col.len(),
            Column::Map(col) => col.len(),
            Column::Bitmap(col) => col.len(),
            Column::Nullable(col) => col.len(),
            Column::Tuple(fields) => fields[0].len(),
            Column::Variant(col) => col.len(),
            Column::Opaque(col) => col.len(),
            Column::Geometry(col) => col.len(),
            Column::Geography(col) => col.len(),
            Column::Vector(col) => col.len(),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef<'_>> {
        match self {
            Column::Null { .. } => Some(ScalarRef::Null),
            Column::EmptyArray { .. } => Some(ScalarRef::EmptyArray),
            Column::EmptyMap { .. } => Some(ScalarRef::EmptyMap),
            Column::Number(col) => Some(ScalarRef::Number(col.index(index)?)),
            Column::Decimal(col) => Some(ScalarRef::Decimal(col.index(index)?)),
            Column::Boolean(col) => Some(ScalarRef::Boolean(col.get(index)?)),
            Column::Binary(col) => Some(ScalarRef::Binary(col.index(index)?)),
            Column::String(col) => Some(ScalarRef::String(col.value(index))),
            Column::Timestamp(col) => Some(ScalarRef::Timestamp(col.get(index).cloned()?)),
            Column::TimestampTz(col) => Some(ScalarRef::TimestampTz(col.get(index).cloned()?)),
            Column::Date(col) => Some(ScalarRef::Date(col.get(index).cloned()?)),
            Column::Interval(col) => Some(ScalarRef::Interval(col.get(index).cloned()?)),
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
            Column::Geometry(col) => Some(ScalarRef::Geometry(col.index(index)?)),
            Column::Geography(col) => Some(ScalarRef::Geography(col.index(index)?)),
            Column::Vector(col) => Some(ScalarRef::Vector(col.index(index)?)),
            Column::Opaque(col) => col.index(index).map(ScalarRef::Opaque),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef<'_> {
        unsafe {
            match self {
                Column::Null { .. } => ScalarRef::Null,
                Column::EmptyArray { .. } => ScalarRef::EmptyArray,
                Column::EmptyMap { .. } => ScalarRef::EmptyMap,
                Column::Number(col) => ScalarRef::Number(col.index_unchecked(index)),
                Column::Decimal(col) => ScalarRef::Decimal(col.index_unchecked(index)),
                Column::Boolean(col) => ScalarRef::Boolean(col.get_bit_unchecked(index)),
                Column::Binary(col) => ScalarRef::Binary(col.index_unchecked(index)),
                Column::String(col) => ScalarRef::String(col.index_unchecked(index)),
                Column::Timestamp(col) => ScalarRef::Timestamp(*col.get_unchecked(index)),
                Column::TimestampTz(col) => ScalarRef::TimestampTz(*col.get_unchecked(index)),
                Column::Date(col) => ScalarRef::Date(*col.get_unchecked(index)),
                Column::Interval(col) => ScalarRef::Interval(*col.get_unchecked(index)),
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
                Column::Geometry(col) => ScalarRef::Geometry(col.index_unchecked(index)),
                Column::Geography(col) => ScalarRef::Geography(col.index_unchecked(index)),
                Column::Vector(col) => ScalarRef::Vector(col.index_unchecked(index)),
                Column::Opaque(col) => ScalarRef::Opaque(col.index_unchecked(index)),
            }
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
            Column::Binary(col) => Column::Binary(col.slice(range)),
            Column::String(col) => {
                Column::String(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Timestamp(col) => {
                Column::Timestamp(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::TimestampTz(col) => {
                Column::TimestampTz(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Date(col) => {
                Column::Date(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Interval(col) => {
                Column::Interval(col.clone().sliced(range.start, range.end - range.start))
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
            Column::Geometry(col) => Column::Geometry(col.slice(range)),
            Column::Geography(col) => Column::Geography(col.slice(range)),
            Column::Vector(col) => Column::Vector(col.slice(range)),
            Column::Opaque(col) => Column::Opaque(col.slice(range)),
        }
    }

    pub fn iter(&self) -> ColumnIterator<'_> {
        ColumnIterator {
            column: self,
            index: 0,
            len: self.len(),
        }
    }

    pub fn domain(&self) -> Domain {
        if self.len() == 0 {
            if matches!(self, Column::Array(_)) {
                return Domain::Array(None);
            }
            if matches!(self, Column::Map(_)) {
                return Domain::Map(None);
            }
            return Domain::full(&self.data_type());
        }

        match self {
            Column::Null { .. } => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Column::EmptyArray { .. } => Domain::Array(None),
            Column::EmptyMap { .. } => Domain::Map(None),

            Column::Number(col) => Domain::Number(col.domain()),
            Column::Decimal(col) => Domain::Decimal(col.domain()),
            Column::Boolean(col) => Domain::Boolean(BooleanDomain {
                has_false: col.null_count() > 0,
                has_true: col.len() - col.null_count() > 0,
            }),
            Column::String(col) => {
                let (min, max) = StringType::iter_column(col).minmax().into_option().unwrap();
                Domain::String(StringDomain {
                    min: min.to_string(),
                    max: Some(max.to_string()),
                })
            }
            Column::Timestamp(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Timestamp(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::TimestampTz(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::TimestampTz(SimpleDomain {
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
            Column::Interval(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Interval(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Array(col) => {
                if col.len() == 0 {
                    Domain::Array(None)
                } else {
                    let inner_domain = col.underlying_column().domain();
                    Domain::Array(Some(Box::new(inner_domain)))
                }
            }
            Column::Map(col) => {
                if col.len() == 0 {
                    Domain::Map(None)
                } else {
                    let inner_domain = col.underlying_column().domain();
                    Domain::Map(Some(Box::new(inner_domain)))
                }
            }
            Column::Nullable(col) => {
                let inner_domain = if col.validity.null_count() > 0 {
                    // goes into the slower path, we will create a new column without nulls
                    let inner = col.column.clone().filter(&col.validity);
                    inner.domain()
                } else {
                    col.column.domain()
                };
                Domain::Nullable(NullableDomain {
                    has_null: col.validity.null_count() > 0,
                    value: Some(Box::new(inner_domain)),
                })
            }
            Column::Tuple(fields) => {
                let domains = fields.iter().map(|col| col.domain()).collect::<Vec<_>>();
                Domain::Tuple(domains)
            }
            Column::Binary(_)
            | Column::Bitmap(_)
            | Column::Variant(_)
            | Column::Geometry(_)
            | Column::Geography(_)
            | Column::Vector(_)
            | Column::Opaque(_) => Domain::Undefined,
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
                DecimalColumn::DECIMAL_TYPE(_, size) => DataType::Decimal(*size),
            }),
            Column::Boolean(_) => DataType::Boolean,
            Column::Binary(_) => DataType::Binary,
            Column::String(_) => DataType::String,
            Column::Timestamp(_) => DataType::Timestamp,
            Column::TimestampTz(_) => DataType::TimestampTz,
            Column::Date(_) => DataType::Date,
            Column::Interval(_) => DataType::Interval,
            Column::Array(array) => {
                let inner = array.values().data_type();
                DataType::Array(Box::new(inner))
            }
            Column::Map(col) => {
                let inner = col.values().data_type();
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
            Column::Geometry(_) => DataType::Geometry,
            Column::Geography(_) => DataType::Geography,
            Column::Vector(col) => DataType::Vector(col.data_type()),
            Column::Opaque(col) => col.data_type(),
        }
    }

    pub fn check_valid(&self) -> Result<()> {
        match self {
            Column::Binary(x) => Ok(x.check_valid()?),
            Column::Variant(x) => Ok(x.check_valid()?),
            Column::Geometry(x) => Ok(x.check_valid()?),
            Column::Geography(x) => Ok(x.check_valid()?),
            Column::Bitmap(x) => Ok(x.check_valid()?),
            Column::Map(x) => {
                for y in x.iter() {
                    y.check_valid()?;
                }
                Ok(())
            }
            Column::Array(x) => x.check_valid(),
            Column::Nullable(x) => {
                if x.column.len() != x.validity.len() {
                    return Err(ErrorCode::Internal(
                        "column and validity length mismatch".to_string(),
                    ));
                }
                x.column.check_valid()
            }
            Column::Tuple(x) => {
                for y in x.iter() {
                    y.check_valid()?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn random(ty: &DataType, len: usize, options: Option<RandomOptions>) -> Self {
        use rand::Rng;
        use rand::SeedableRng;
        use rand::distributions::Alphanumeric;
        use rand::rngs::SmallRng;
        let mut rng = match &options {
            Some(RandomOptions {
                seed: Some(seed), ..
            }) => SmallRng::seed_from_u64(*seed),
            _ => SmallRng::from_entropy(),
        };

        let min_string_len = options.as_ref().map(|opt| opt.min_string_len).unwrap_or(0);
        let max_string_len = options.as_ref().map(|opt| opt.max_string_len).unwrap_or(5);
        let max_arr_len = options.as_ref().map(|opt| opt.max_array_len).unwrap_or(3);

        match ty {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            DataType::Boolean => {
                BooleanType::from_data((0..len).map(|_| rng.gen_bool(0.5)).collect_vec())
            }
            DataType::Binary => BinaryType::from_data(
                (0..len)
                    .map(|_| {
                        let mut rng = match &options {
                            Some(RandomOptions {
                                seed: Some(seed), ..
                            }) => SmallRng::seed_from_u64(*seed),
                            _ => SmallRng::from_entropy(),
                        };
                        let str_len = rng.gen_range(min_string_len..=max_string_len);
                        rng.sample_iter(&Alphanumeric)
                            // randomly generate 5 characters.
                            .take(str_len)
                            .collect::<Vec<_>>()
                    })
                    .collect_vec(),
            ),
            DataType::String => StringType::from_data(
                (0..len)
                    .map(|_| {
                        let mut rng = match &options {
                            Some(RandomOptions {
                                seed: Some(seed), ..
                            }) => SmallRng::seed_from_u64(*seed),
                            _ => SmallRng::from_entropy(),
                        };
                        let str_len = rng.gen_range(min_string_len..=max_string_len);
                        rng.sample_iter(&Alphanumeric)
                            // randomly generate 5 characters.
                            .take(str_len)
                            .map(char::from)
                            .collect::<String>()
                    })
                    .collect_vec(),
            ),
            DataType::Number(num_ty) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => {
                        NumberType::<NUM_TYPE>::from_data(
                            (0..len).map(|_| rng.r#gen::<NUM_TYPE>()).collect_vec(),
                        )
                    }
                })
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match DecimalDataType::from(*size) {
                    DecimalDataType::DECIMAL(size) => {
                        let values = (0..len)
                            .map(|_| DECIMAL::from(rng.r#gen::<i16>()))
                            .collect::<Vec<DECIMAL>>();
                        <DECIMAL as Decimal>::upcast_column(values.into(), size)
                    }
                })
            }
            DataType::Timestamp => TimestampType::from_data(
                (0..len)
                    .map(|_| rng.gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX))
                    .collect::<Vec<i64>>(),
            ),
            DataType::TimestampTz => TimestampTzType::from_data(
                (0..len)
                    .map(|_| {
                        let i = rng.gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX);
                        timestamp_tz::new(i, 0)
                    })
                    .collect::<Vec<timestamp_tz>>(),
            ),
            DataType::Date => DateType::from_data(
                (0..len)
                    .map(|_| rng.gen_range(DATE_MIN..=DATE_MAX))
                    .collect::<Vec<i32>>(),
            ),
            DataType::Interval => IntervalType::from_data(Vec::from_iter(
                std::iter::repeat_with(|| {
                    let normal = rand_distr::Normal::new(0.001, 1.0).unwrap();
                    const MAX_MONTHS: i64 = i64::MAX / months_days_micros::MICROS_PER_MONTH;
                    const MAX_DAYS: i64 = i64::MAX / months_days_micros::MICROS_PER_DAY;
                    months_days_micros::new(
                        (rng.sample(normal) * 0.3 * MAX_MONTHS as f64) as i32,
                        (rng.sample(normal) * 0.3 * MAX_DAYS as f64) as i32,
                        (rng.sample(normal) * 2.0 * months_days_micros::MICROS_PER_DAY as f64)
                            as i64,
                    )
                })
                .take(len),
            )),
            DataType::Nullable(ty) => {
                let column = Column::random(ty, len, options);
                let bitmap =
                    Bitmap::from((0..len).map(|_| rng.gen_bool(0.5)).collect::<Vec<bool>>());

                // If the value is NULL, insert default value in underlying column
                let mut builder = ColumnBuilder::with_capacity(ty, len);
                for (valid, value) in bitmap.iter().zip(column.iter()) {
                    if valid {
                        builder.push(value);
                    } else {
                        builder.push_default();
                    }
                }
                let new_column = builder.build();
                NullableColumn::new_column(new_column, bitmap)
            }
            DataType::Array(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(0);
                for _ in 0..len {
                    inner_len += rng.gen_range(0..=max_arr_len) as u64;
                    offsets.push(inner_len);
                }
                let inner_column = Column::random(inner_ty, inner_len as usize, options);
                Column::Array(Box::new(ArrayColumn::new(inner_column, offsets.into())))
            }
            DataType::Map(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(0);
                for _ in 0..len {
                    inner_len += rng.gen_range(0..=max_arr_len) as u64;
                    offsets.push(inner_len);
                }
                let inner_column = Column::random(inner_ty, inner_len as usize, options);
                Column::Map(Box::new(ArrayColumn::new(inner_column, offsets.into())))
            }
            DataType::Bitmap => BitmapType::from_data(
                (0..len)
                    .map(|_| {
                        let data: [u64; 4] = rng.r#gen();
                        let rb = HybridBitmap::from_iter(data.iter());
                        let mut buf = vec![];
                        rb.serialize_into(&mut buf)
                            .expect("failed serialize hybrid bitmap");
                        buf
                    })
                    .collect_vec(),
            ),
            DataType::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|ty| Column::random(ty, len, options.clone()))
                    .collect::<Vec<_>>();
                Column::Tuple(fields)
            }
            DataType::Variant => {
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    let val = jsonb::Value::rand_value();
                    data.push(val.to_vec());
                }
                VariantType::from_data(data)
            }
            DataType::Geometry => {
                let mut data = Vec::with_capacity(len);
                (0..len).for_each(|_| {
                    let x = rng.r#gen::<f64>();
                    let y = rng.r#gen::<f64>();
                    let val = Point::new(x, y);
                    data.push(
                        Geometry::from(val)
                            .to_ewkb(CoordDimensions::xy(), None)
                            .unwrap(),
                    );
                });
                GeometryType::from_data(data)
            }
            DataType::Geography => {
                use databend_common_io::geography;

                let mut builder = GeographyType::create_builder(len, &[]);
                for _ in 0..len {
                    let lon = rng.gen_range(geography::LONGITUDE_MIN..=geography::LONGITUDE_MAX);
                    let lat = rng.gen_range(geography::LATITUDE_MIN..=geography::LATITUDE_MAX);
                    builder.put_slice(&GeographyType::point(lon, lat).0);
                    builder.commit_row()
                }
                Column::Geography(GeographyColumn(builder.build()))
            }
            DataType::Vector(vector_ty) => {
                let mut builder = VectorColumnBuilder::with_capacity(vector_ty, len);
                match vector_ty {
                    VectorDataType::Int8(dimension) => {
                        for _ in 0..len {
                            let value = (0..*dimension).map(|_| rng.r#gen::<i8>()).collect_vec();
                            let scalar = VectorScalarRef::Int8(&value);
                            builder.push(&scalar);
                        }
                    }
                    VectorDataType::Float32(dimension) => {
                        for _ in 0..len {
                            let value = (0..*dimension).map(|_| rng.r#gen::<F32>()).collect_vec();
                            let scalar = VectorScalarRef::Float32(&value);
                            builder.push(&scalar);
                        }
                    }
                }
                Column::Vector(builder.build())
            }
            DataType::Generic(_) | DataType::StageLocation => unreachable!(),
            DataType::Opaque(size) => {
                with_opaque_size!(|N| match *size {
                    N => {
                        let mut builder = Vec::with_capacity(len);
                        for _ in 0..len {
                            let mut value = [0u64; N];
                            for i in 0..N {
                                value[i] = rng.r#gen::<u64>();
                            }
                            builder.push(value);
                        }
                        OpaqueType::<N>::upcast_column_with_type(builder.into(), ty)
                    }
                    _ => unreachable!("Unsupported Opaque size: {}", size),
                })
            }
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
                NullableColumn::new_column(null_column.column, validity)
            }
            _ => {
                let validity = validity.unwrap_or_else(|| Bitmap::new_constant(true, self.len()));
                NullableColumn::new_column(self, validity)
            }
        }
    }

    pub fn memory_size(&self, gc: bool) -> usize {
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
            Column::Decimal(DecimalColumn::Decimal64(col, _)) => col.len() * 8,
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col.len() * 32,
            Column::Boolean(c) => c.as_slice().0.len(),
            Column::Binary(col) => col.memory_size(),
            Column::String(col) => col.memory_size(gc),
            Column::Timestamp(col) => col.len() * 8,
            Column::TimestampTz(col) => col.len() * 16,
            Column::Date(col) => col.len() * 4,
            Column::Interval(col) => col.len() * 16,
            Column::Array(col) => col.memory_size(gc),
            Column::Map(col) => col.memory_size(gc),
            Column::Bitmap(col) => col.memory_size(),
            Column::Nullable(c) => c.column.memory_size(gc) + c.validity.as_slice().0.len(),
            Column::Tuple(fields) => fields.iter().map(|f| f.memory_size(gc)).sum(),
            Column::Variant(col) => col.memory_size(),
            Column::Geometry(col) => col.memory_size(),
            Column::Geography(col) => GeographyType::column_memory_size(col, gc),
            Column::Vector(col) => col.memory_size(),
            Column::Opaque(col) => col.memory_size(),
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
            Column::TimestampTz(col) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal64(col, _)) => col.len() * 8,
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col.len() * 32,
            Column::Interval(col) => col.len() * 16,
            Column::Geography(col) => GeographyType::column_memory_size(col, false),
            Column::Boolean(c) => c.len(),
            // 8 * len + size of bytes
            Column::Binary(col)
            | Column::Bitmap(col)
            | Column::Variant(col)
            | Column::Geometry(col) => col.memory_size(),
            Column::String(col) => col.len() * 8 + col.total_bytes_len(),
            Column::Array(col) | Column::Map(col) => {
                col.underlying_column().serialize_size() + col.len() * 8
            }
            Column::Nullable(c) => c.column.serialize_size() + c.len(),
            Column::Tuple(fields) => fields.iter().map(|f| f.serialize_size()).sum(),
            Column::Vector(col) => col.memory_size(),
            Column::Opaque(col) => col.memory_size(), // Same as memory size for opaque types
        }
    }

    /// Returns (is_all_null, Option bitmap)
    pub fn validity(&self) -> (bool, Option<&Bitmap>) {
        match self {
            Column::Null { .. } => (true, None),
            Column::Nullable(c) => {
                if c.validity.null_count() == c.validity.len() {
                    (true, Some(&c.validity))
                } else {
                    (false, Some(&c.validity))
                }
            }
            _ => (false, None),
        }
    }

    /// Checks if the average length of a string column exceeds 256 bytes.
    /// If it does, the bloom index for the column will not be established.
    pub fn check_large_string(&self) -> bool {
        let (inner, len) = if let Column::Nullable(c) = self {
            (&c.column, c.validity.true_count())
        } else {
            (self, self.len())
        };
        if let Column::String(v) = inner {
            let bytes_per_row = v.total_bytes_len() / len.max(1);
            if bytes_per_row > 256 {
                return true;
            }
        }
        false
    }
}

/// Serialize a column to a base64 string.
/// Because we may use serde::json/bincode to serialize the column, so we wrap it into string
impl Serialize for Column {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        let bytes = serialize_column(self);
        let base64_str = general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Column, D::Error>
    where D: Deserializer<'de> {
        struct ColumnVisitor;

        impl Visitor<'_> for ColumnVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an arrow chunk with exactly one column")
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
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

impl BorshSerialize for Column {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = serialize_column(self);
        BorshSerialize::serialize(&bytes, writer)
    }
}

impl BorshDeserialize for Column {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let bytes: Vec<u8> = borsh::BorshDeserialize::deserialize_reader(reader)?;
        let column =
            deserialize_column(&bytes).expect("expecting an arrow chunk with exactly one column");
        Ok(column)
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
            Column::Binary(col) => ColumnBuilder::Binary(BinaryColumnBuilder::from_column(col)),
            Column::String(col) => ColumnBuilder::String(StringColumnBuilder::from_column(col)),
            Column::Timestamp(col) => ColumnBuilder::Timestamp(buffer_into_mut(col)),
            Column::TimestampTz(col) => ColumnBuilder::TimestampTz(buffer_into_mut(col)),
            Column::Date(col) => ColumnBuilder::Date(buffer_into_mut(col)),
            Column::Interval(col) => ColumnBuilder::Interval(buffer_into_mut(col)),
            Column::Array(box col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Map(box col) => {
                ColumnBuilder::Map(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Bitmap(col) => ColumnBuilder::Bitmap(BinaryColumnBuilder::from_column(col)),
            Column::Nullable(box col) => {
                ColumnBuilder::Nullable(Box::new(NullableColumnBuilder::from_column(col)))
            }
            Column::Tuple(fields) => ColumnBuilder::Tuple(
                fields
                    .iter()
                    .map(|col| ColumnBuilder::from_column(col.clone()))
                    .collect(),
            ),
            Column::Variant(col) => ColumnBuilder::Variant(BinaryColumnBuilder::from_column(col)),
            Column::Geometry(col) => ColumnBuilder::Geometry(BinaryColumnBuilder::from_column(col)),
            Column::Geography(col) => {
                ColumnBuilder::Geography(GeographyType::column_to_builder(col))
            }
            Column::Vector(col) => ColumnBuilder::Vector(VectorColumnBuilder::from_column(col)),
            Column::Opaque(col) => {
                ColumnBuilder::Opaque(crate::types::opaque::OpaqueColumnBuilder::from_column(col))
            }
        }
    }

    pub fn repeat(scalar: &ScalarRef, n: usize, data_type: &DataType) -> ColumnBuilder {
        if !scalar.is_null() {
            if let DataType::Nullable(ty) = data_type {
                let mut builder = ColumnBuilder::with_capacity(ty, 1);
                builder.push_repeat(scalar, n);
                return ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                    builder,
                    validity: Bitmap::new_constant(true, n).make_mut(),
                }));
            }
        }

        match scalar {
            ScalarRef::Null => match data_type {
                DataType::Null => ColumnBuilder::Null { len: n },
                DataType::Nullable(inner) => {
                    ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                        builder: Self::repeat_default(inner, n),
                        validity: MutableBitmap::from_len_zeroed(n),
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
            ScalarRef::Binary(s) => ColumnBuilder::Binary(BinaryColumnBuilder::repeat(s, n)),
            ScalarRef::String(s) => ColumnBuilder::String(StringColumnBuilder::repeat(s, n)),
            ScalarRef::Timestamp(d) => ColumnBuilder::Timestamp(vec![*d; n]),
            ScalarRef::TimestampTz(d) => ColumnBuilder::TimestampTz(vec![*d; n]),
            ScalarRef::Date(d) => ColumnBuilder::Date(vec![*d; n]),
            ScalarRef::Interval(i) => ColumnBuilder::Interval(vec![*i; n]),
            ScalarRef::Array(col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::repeat(col, n)))
            }
            ScalarRef::Map(col) => ColumnBuilder::Map(Box::new(ArrayColumnBuilder::repeat(col, n))),
            ScalarRef::Bitmap(b) => ColumnBuilder::Bitmap(BinaryColumnBuilder::repeat(b, n)),
            ScalarRef::Tuple(fields) => {
                let DataType::Tuple(fields_ty) = data_type else {
                    unreachable!()
                };
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .zip(fields_ty)
                        .map(|(field, ty)| ColumnBuilder::repeat(field, n, ty))
                        .collect(),
                )
            }
            ScalarRef::Variant(s) => ColumnBuilder::Variant(BinaryColumnBuilder::repeat(s, n)),
            ScalarRef::Geometry(s) => ColumnBuilder::Geometry(BinaryColumnBuilder::repeat(s, n)),
            ScalarRef::Geography(s) => {
                ColumnBuilder::Geography(BinaryColumnBuilder::repeat(s.0, n))
            }
            ScalarRef::Vector(s) => ColumnBuilder::Vector(VectorColumnBuilder::repeat(s, n)),
            ScalarRef::Opaque(v) => {
                with_opaque_type!(|T| match v {
                    OpaqueScalarRef::T(arr) => {
                        ColumnBuilder::Opaque(OpaqueColumnBuilder::T(vec![**arr; n]))
                    }
                })
            }
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
            ColumnBuilder::Binary(builder) => builder.len(),
            ColumnBuilder::String(builder) => builder.len(),
            ColumnBuilder::Timestamp(builder) => builder.len(),
            ColumnBuilder::TimestampTz(builder) => builder.len(),
            ColumnBuilder::Date(builder) => builder.len(),
            ColumnBuilder::Interval(builder) => builder.len(),
            ColumnBuilder::Array(builder) => builder.len(),
            ColumnBuilder::Map(builder) => builder.len(),
            ColumnBuilder::Bitmap(builder) => builder.len(),
            ColumnBuilder::Nullable(builder) => builder.len(),
            ColumnBuilder::Tuple(fields) => fields[0].len(),
            ColumnBuilder::Variant(builder) => builder.len(),
            ColumnBuilder::Geometry(builder) => builder.len(),
            ColumnBuilder::Geography(builder) => builder.len(),
            ColumnBuilder::Vector(builder) => builder.len(),
            ColumnBuilder::Opaque(builder) => builder.len(),
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
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal64(builder, _)) => {
                builder.len() * 8
            }
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(builder, _)) => {
                builder.len() * 16
            }
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(builder, _)) => {
                builder.len() * 32
            }
            ColumnBuilder::Boolean(c) => c.as_slice().len(),
            ColumnBuilder::Binary(col) => col.memory_size(),
            ColumnBuilder::String(col) => col.memory_size(),
            ColumnBuilder::Timestamp(col) => col.len() * 8,
            ColumnBuilder::TimestampTz(col) => col.len() * 16,
            ColumnBuilder::Date(col) => col.len() * 4,
            ColumnBuilder::Interval(col) => col.len() * 16,
            ColumnBuilder::Array(b) => b.builder.memory_size() + b.offsets.len() * 8,
            ColumnBuilder::Map(b) => b.builder.memory_size() + b.offsets.len() * 8,
            ColumnBuilder::Bitmap(b) => b.memory_size(),
            ColumnBuilder::Nullable(b) => b.builder.memory_size() + b.validity.as_slice().len(),
            ColumnBuilder::Tuple(fields) => fields.iter().map(|f| f.memory_size()).sum(),
            ColumnBuilder::Variant(b) => b.memory_size(),
            ColumnBuilder::Geometry(b) => b.memory_size(),
            ColumnBuilder::Geography(b) => b.memory_size(),
            ColumnBuilder::Vector(b) => b.memory_size(),
            ColumnBuilder::Opaque(b) => b.memory_size(),
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
                DecimalColumnBuilder::DECIMAL_TYPE(_, size) => DataType::Decimal(*size),
            }),
            ColumnBuilder::Boolean(_) => DataType::Boolean,
            ColumnBuilder::Binary(_) => DataType::Binary,
            ColumnBuilder::String(_) => DataType::String,
            ColumnBuilder::Timestamp(_) => DataType::Timestamp,
            ColumnBuilder::TimestampTz(_) => DataType::TimestampTz,
            ColumnBuilder::Date(_) => DataType::Date,
            ColumnBuilder::Interval(_) => DataType::Interval,
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
            ColumnBuilder::Geometry(_) => DataType::Geometry,
            ColumnBuilder::Geography(_) => DataType::Geography,
            ColumnBuilder::Vector(col) => DataType::Vector(col.data_type()),
            ColumnBuilder::Opaque(col) => col.data_type(),
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
            DataType::Decimal(size) => ColumnBuilder::Decimal(DecimalColumnBuilder::with_capacity(
                &(*size).into(),
                capacity,
            )),
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::with_capacity(capacity)),
            DataType::Binary => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Binary(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::String => ColumnBuilder::String(StringColumnBuilder::with_capacity(capacity)),
            DataType::Timestamp => ColumnBuilder::Timestamp(Vec::with_capacity(capacity)),
            DataType::TimestampTz => ColumnBuilder::TimestampTz(Vec::with_capacity(capacity)),
            DataType::Date => ColumnBuilder::Date(Vec::with_capacity(capacity)),
            DataType::Interval => ColumnBuilder::Interval(Vec::with_capacity(capacity)),
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
                ColumnBuilder::Bitmap(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Variant => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Variant(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Geometry => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Geometry(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Geography => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Geography(BinaryColumnBuilder::with_capacity(
                    capacity,
                    data_capacity,
                ))
            }
            DataType::Vector(vector_ty) => {
                ColumnBuilder::Vector(VectorColumnBuilder::with_capacity(vector_ty, capacity))
            }
            DataType::Opaque(size) => {
                with_opaque_size_mapped!(|N| match *size {
                    N =>
                        ColumnBuilder::Opaque(OpaqueColumnBuilder::N(Vec::with_capacity(capacity,))),
                    _ => unreachable!("Unsupported Opaque size: {}", size),
                })
            }
            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
            DataType::StageLocation => {
                unreachable!("unable to initialize column builder for stage location type")
            }
        }
    }

    pub fn repeat_default(ty: &DataType, len: usize) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len },
            DataType::EmptyMap => ColumnBuilder::EmptyMap { len },

            // bitmap
            DataType::Nullable(inner) => ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                builder: Self::repeat_default(inner, len),
                validity: MutableBitmap::from_len_zeroed(len),
            })),
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::from_len_zeroed(len)),

            // number based
            DataType::Number(num_ty) => {
                ColumnBuilder::Number(NumberColumnBuilder::repeat_default(num_ty, len))
            }
            DataType::Decimal(size) => {
                ColumnBuilder::Decimal(DecimalColumnBuilder::repeat_default(&(*size).into(), len))
            }
            DataType::Timestamp => ColumnBuilder::Timestamp(vec![0; len]),
            DataType::TimestampTz => ColumnBuilder::TimestampTz(vec![timestamp_tz::new(0, 0); len]),
            DataType::Date => ColumnBuilder::Date(vec![0; len]),
            DataType::Interval => {
                ColumnBuilder::Interval(vec![months_days_micros::new(0, 0, 0); len])
            }

            // binary based
            DataType::Binary => ColumnBuilder::Binary(BinaryColumnBuilder::repeat_default(len)),
            DataType::String => ColumnBuilder::String(StringColumnBuilder::repeat_default(len)),
            DataType::Bitmap => ColumnBuilder::Bitmap(BinaryColumnBuilder::repeat_default(len)),
            DataType::Variant => ColumnBuilder::Variant(BinaryColumnBuilder::repeat_default(len)),
            DataType::Geometry => ColumnBuilder::Geometry(BinaryColumnBuilder::repeat_default(len)),
            DataType::Geography => {
                ColumnBuilder::Geography(BinaryColumnBuilder::repeat_default(len))
            }

            DataType::Opaque(size) => {
                with_opaque_size_mapped!(|T| match *size {
                    T => ColumnBuilder::Opaque(OpaqueColumnBuilder::T(vec![
                        unsafe {
                            std::mem::zeroed()
                        };
                        len
                    ])),
                    _ => unreachable!("Unsupported Opaque size: {}", size),
                })
            }

            DataType::Array(ty) => ColumnBuilder::Array(Box::new(ArrayColumnBuilder {
                builder: Self::with_capacity(ty, 0),
                offsets: vec![0; len + 1],
            })),
            DataType::Map(ty) => ColumnBuilder::Map(Box::new(ArrayColumnBuilder {
                builder: Self::with_capacity(ty, 0),
                offsets: vec![0; len + 1],
            })),
            DataType::Tuple(fields) => {
                assert!(!fields.is_empty());
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .map(|field| Self::repeat_default(field, len))
                        .collect(),
                )
            }
            DataType::Vector(vector_ty) => {
                ColumnBuilder::Vector(VectorColumnBuilder::repeat_default(vector_ty, len))
            }
            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
            DataType::StageLocation => {
                unreachable!("unable to initialize column builder for stage location type")
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
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => {
                BooleanType::push_item(builder, value);
            }
            (ColumnBuilder::Binary(builder), ScalarRef::Binary(value)) => {
                BinaryType::push_item(builder, value);
            }
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                StringType::push_item(builder, value)
            }
            (ColumnBuilder::Timestamp(builder), ScalarRef::Timestamp(value)) => {
                TimestampType::push_item(builder, value)
            }
            (ColumnBuilder::TimestampTz(builder), ScalarRef::TimestampTz(value)) => {
                TimestampTzType::push_item(builder, value)
            }
            (ColumnBuilder::Date(builder), ScalarRef::Date(value)) => {
                DateType::push_item(builder, value)
            }
            (ColumnBuilder::Interval(builder), ScalarRef::Interval(value)) => {
                IntervalType::push_item(builder, value)
            }
            (ColumnBuilder::Array(builder), ScalarRef::Array(value)) => {
                ArrayType::push_item(builder, value);
            }
            (ColumnBuilder::Map(builder), ScalarRef::Map(value)) => {
                ArrayType::push_item(builder, value);
            }
            (ColumnBuilder::Bitmap(builder), ScalarRef::Bitmap(value)) => {
                BitmapType::push_item(builder, value);
            }
            (ColumnBuilder::Nullable(builder), ScalarRef::Null) => {
                builder.push_null();
            }
            (ColumnBuilder::Nullable(builder), scalar) => {
                builder.push(scalar);
            }
            (ColumnBuilder::Tuple(fields), ScalarRef::Tuple(value)) => {
                assert_eq!(fields.len(), value.len());
                for (field, scalar) in fields.iter_mut().zip(value.into_iter()) {
                    field.push(scalar);
                }
            }
            (ColumnBuilder::Variant(builder), ScalarRef::Variant(value)) => {
                VariantType::push_item(builder, value);
            }
            (ColumnBuilder::Geometry(builder), ScalarRef::Geometry(value)) => {
                GeometryType::push_item(builder, value);
            }
            (ColumnBuilder::Geography(builder), ScalarRef::Geography(value)) => {
                GeographyType::push_item(builder, value);
            }
            (ColumnBuilder::Vector(builder), ScalarRef::Vector(value)) => {
                builder.push(&value);
            }
            (ColumnBuilder::Opaque(builder), ScalarRef::Opaque(value)) => {
                with_opaque_type!(|T| match (builder, value) {
                    (OpaqueColumnBuilder::T(builder), OpaqueScalarRef::T(value)) => {
                        builder.push(*value);
                    }
                    _ => unreachable!("Mismatched Opaque types"),
                })
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        }
    }

    pub fn push_repeat(&mut self, item: &ScalarRef, n: usize) {
        match (self, item) {
            (ColumnBuilder::Null { len }, ScalarRef::Null)
            | (ColumnBuilder::EmptyArray { len }, ScalarRef::EmptyArray)
            | (ColumnBuilder::EmptyMap { len }, ScalarRef::EmptyMap) => *len += n,

            (ColumnBuilder::Number(builder), ScalarRef::Number(value)) => {
                builder.push_repeat(*value, n)
            }
            (ColumnBuilder::Decimal(builder), ScalarRef::Decimal(value)) => {
                builder.push_repeat(*value, n)
            }
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => {
                BooleanType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Timestamp(builder), ScalarRef::Timestamp(value)) => {
                TimestampType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::TimestampTz(builder), ScalarRef::TimestampTz(value)) => {
                TimestampTzType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Interval(builder), ScalarRef::Interval(value)) => {
                IntervalType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Date(builder), ScalarRef::Date(value)) => {
                DateType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Binary(builder), ScalarRef::Binary(value)) => {
                BinaryType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                StringType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Array(builder), ScalarRef::Array(value))
            | (ColumnBuilder::Map(builder), ScalarRef::Map(value)) => {
                builder.push_repeat(value, n);
            }
            (ColumnBuilder::Bitmap(builder), ScalarRef::Bitmap(value)) => {
                BitmapType::push_item_repeat(builder, value, n);
            }
            (ColumnBuilder::Nullable(builder), ScalarRef::Null) => {
                NullableType::push_item_repeat(builder, None, n);
            }
            (ColumnBuilder::Nullable(builder), value) => {
                builder.push_repeat(value.clone(), n);
            }
            (ColumnBuilder::Tuple(fields), ScalarRef::Tuple(value)) => {
                assert_eq!(fields.len(), value.len());
                for (field, scalar) in fields.iter_mut().zip(value.iter()) {
                    field.push_repeat(scalar, n)
                }
            }
            (ColumnBuilder::Variant(builder), ScalarRef::Variant(value)) => {
                VariantType::push_item_repeat(builder, value, n);
            }
            (ColumnBuilder::Geometry(builder), ScalarRef::Geometry(value)) => {
                GeometryType::push_item_repeat(builder, value, n);
            }
            (ColumnBuilder::Geography(builder), ScalarRef::Geography(value)) => {
                GeographyType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Vector(builder), ScalarRef::Vector(value)) => {
                builder.push_repeat(value, n);
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        };
    }

    pub fn push_default(&mut self) {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::EmptyMap { len } => *len += 1,
            ColumnBuilder::Number(builder) => builder.push_default(),
            ColumnBuilder::Decimal(builder) => builder.push_default(),
            ColumnBuilder::Boolean(builder) => builder.push(false),
            ColumnBuilder::Binary(builder) => builder.commit_row(),
            ColumnBuilder::String(builder) => builder.commit_row(),
            ColumnBuilder::Timestamp(builder) => builder.push(0),
            ColumnBuilder::TimestampTz(builder) => builder.push(timestamp_tz::new(0, 0)),
            ColumnBuilder::Date(builder) => builder.push(0),
            ColumnBuilder::Interval(builder) => builder.push(months_days_micros::new(0, 0, 0)),
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
            ColumnBuilder::Geometry(builder) => builder.commit_row(),
            ColumnBuilder::Geography(builder) => builder.commit_row(),
            ColumnBuilder::Vector(builder) => builder.push_default(),
            ColumnBuilder::Opaque(builder) => builder.push_default(),
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
            ColumnBuilder::Binary(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder)
            | ColumnBuilder::Geometry(builder) => {
                let offset = reader.read_scalar::<u64>()? as usize;
                builder.data.resize(offset + builder.data.len(), 0);
                let last = *builder.offsets.last().unwrap() as usize;
                reader.read_exact(&mut builder.data[last..last + offset])?;
                builder.commit_row();
            }
            ColumnBuilder::Geography(builder) => {
                let offset = reader.read_scalar::<u64>()? as usize;
                builder.data.resize(offset + builder.data.len(), 0);
                let last = *builder.offsets.last().unwrap() as usize;
                reader.read_exact(&mut builder.data[last..last + offset])?;
                builder.commit_row();
            }
            ColumnBuilder::String(builder) => {
                let offset = reader.read_scalar::<u64>()? as usize;
                builder.row_buffer.resize(offset, 0);
                reader.read_exact(&mut builder.row_buffer)?;

                #[cfg(debug_assertions)]
                databend_common_column::binview::CheckUTF8::check_utf8(&builder.row_buffer)
                    .unwrap();

                builder.commit_row();
            }
            ColumnBuilder::Timestamp(builder) => {
                let mut value: i64 = reader.read_scalar()?;
                clamp_timestamp(&mut value);
                builder.push(value);
            }
            ColumnBuilder::TimestampTz(builder) => {
                let value = timestamp_tz(i128::de_binary(reader));
                builder.push(value);
            }
            ColumnBuilder::Date(builder) => {
                let value: i32 = reader.read_scalar()?;
                builder.push(value);
            }
            ColumnBuilder::Interval(builder) => {
                let value = months_days_micros(i128::de_binary(reader));
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
            ColumnBuilder::Vector(builder) => match builder {
                VectorColumnBuilder::Int8((values, dimension)) => {
                    for _ in 0..*dimension {
                        let val = reader.read_scalar::<i8>()?;
                        values.push(val);
                    }
                }
                VectorColumnBuilder::Float32((values, dimension)) => {
                    for _ in 0..*dimension {
                        let val = reader.read_scalar::<F32>()?;
                        values.push(val);
                    }
                }
            },
            ColumnBuilder::Opaque(_) => {
                return Err(ErrorCode::Unimplemented(
                    "Opaque types don't support push_value",
                ));
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
            ColumnBuilder::Binary(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder)
            | ColumnBuilder::Geometry(builder)
            | ColumnBuilder::Geography(builder) => {
                for row in 0..rows {
                    let reader = &reader[step * row..];
                    builder.put_slice(reader);
                    builder.commit_row();
                }
            }
            ColumnBuilder::String(builder) => {
                for row in 0..rows {
                    let bytes = &reader[step * row..];

                    #[cfg(debug_assertions)]
                    databend_common_column::binview::CheckUTF8::check_utf8(&bytes).unwrap();

                    let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                    builder.put_and_commit(s);
                }
            }
            ColumnBuilder::Timestamp(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let mut value: i64 = reader.read_scalar()?;
                    clamp_timestamp(&mut value);
                    builder.push(value);
                }
            }
            ColumnBuilder::TimestampTz(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    builder.push(timestamp_tz(i128::de_binary(&mut reader)));
                }
            }
            ColumnBuilder::Date(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let value: i32 = reader.read_scalar()?;
                    builder.push(value);
                }
            }
            ColumnBuilder::Interval(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    builder.push(months_days_micros(i128::de_binary(&mut reader)));
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
            ColumnBuilder::Vector(builder) => match builder {
                VectorColumnBuilder::Int8((values, dimension)) => {
                    for row in 0..rows {
                        let mut reader = &reader[step * row..];
                        for _ in 0..*dimension {
                            let val = reader.read_scalar::<i8>()?;
                            values.push(val);
                        }
                    }
                }
                VectorColumnBuilder::Float32((values, dimension)) => {
                    for row in 0..rows {
                        let mut reader = &reader[step * row..];
                        for _ in 0..*dimension {
                            let val = reader.read_scalar::<F32>()?;
                            values.push(val);
                        }
                    }
                }
            },
            ColumnBuilder::Opaque(_) => {
                return Err(ErrorCode::Unimplemented(
                    "Opaque types don't support push_fix_len_binaries",
                ));
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
            ColumnBuilder::Binary(builder) => builder.pop().map(Scalar::Binary),
            ColumnBuilder::String(builder) => builder.pop().map(Scalar::String),
            ColumnBuilder::Timestamp(builder) => builder.pop().map(Scalar::Timestamp),
            ColumnBuilder::TimestampTz(builder) => builder.pop().map(Scalar::TimestampTz),
            ColumnBuilder::Date(builder) => builder.pop().map(Scalar::Date),
            ColumnBuilder::Interval(builder) => builder.pop().map(Scalar::Interval),
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
            ColumnBuilder::Geometry(builder) => builder.pop().map(Scalar::Geometry),
            ColumnBuilder::Geography(builder) => {
                builder.pop().map(Geography).map(Scalar::Geography)
            }
            ColumnBuilder::Vector(builder) => builder.pop().map(Scalar::Vector),
            ColumnBuilder::Opaque(builder) => builder.pop().map(Scalar::Opaque),
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
            (ColumnBuilder::Binary(builder), Column::Binary(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::String(builder), Column::String(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Variant(builder), Column::Variant(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Geometry(builder), Column::Geometry(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Geography(builder), Column::Geography(other)) => {
                GeographyType::append_column(builder, other);
            }
            (ColumnBuilder::Timestamp(builder), Column::Timestamp(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::TimestampTz(builder), Column::TimestampTz(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::Date(builder), Column::Date(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::Interval(builder), Column::Interval(other)) => {
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
            (ColumnBuilder::Vector(builder), Column::Vector(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Opaque(builder), Column::Opaque(other)) => {
                with_opaque_type!(|T| match (builder, other) {
                    (OpaqueColumnBuilder::T(builder), OpaqueColumn::T(other)) => {
                        builder.extend(other.iter().copied());
                    }
                    _ => unreachable!("Mismatched Opaque types"),
                })
            }
            (this, other) => unreachable!(
                "unable append column(data type: {:?}) into builder(data type: {:?})",
                other.data_type(),
                this.data_type()
            ),
        }
    }

    pub fn build(self) -> Column {
        match_template::match_template! {
            T = [
                Date => DateType,
                Timestamp => TimestampType,
                TimestampTz => TimestampTzType,
                Interval => IntervalType,
                Boolean => BooleanType,
                Binary => BinaryType,
                String => StringType,
                Bitmap => BitmapType,
                Variant => VariantType,
                Geometry => GeometryType,
                Geography => GeographyType,
            ],
            match self {
                ColumnBuilder::T(b) => {
                    T::upcast_column_with_type(T::build_column(b), &T::data_type())
                }
                ColumnBuilder::Null { len } => Column::Null { len },
                ColumnBuilder::EmptyArray { len } => Column::EmptyArray { len },
                ColumnBuilder::EmptyMap { len } => Column::EmptyMap { len },

                ColumnBuilder::Number(builder) => Column::Number(builder.build()),
                ColumnBuilder::Decimal(builder) => Column::Decimal(builder.build()),
                ColumnBuilder::Array(builder) => Column::Array(Box::new(builder.build())),
                ColumnBuilder::Map(builder) => Column::Map(Box::new(builder.build())),
                ColumnBuilder::Nullable(builder) => Column::Nullable(Box::new(builder.build())),
                ColumnBuilder::Tuple(fields) => {
                    assert!(fields.iter().map(|field| field.len()).all_equal());
                    Column::Tuple(fields.into_iter().map(|field| field.build()).collect())
                }
                ColumnBuilder::Vector(b) => Column::Vector(b.build()),
                ColumnBuilder::Opaque(b) => Column::Opaque(b.build()),
            }
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
            ColumnBuilder::Array(builder) => Scalar::Array(builder.build_scalar()),
            ColumnBuilder::Map(builder) => Scalar::Map(builder.build_scalar()),
            ColumnBuilder::Nullable(builder) => builder.build_scalar().unwrap_or(Scalar::Null),
            ColumnBuilder::Tuple(fields) => Scalar::Tuple(
                fields
                    .into_iter()
                    .map(|field| field.build_scalar())
                    .collect(),
            ),

            ColumnBuilder::Boolean(b) => Scalar::Boolean(BooleanType::build_scalar(b)),
            ColumnBuilder::Binary(b) => Scalar::Binary(BinaryType::build_scalar(b)),
            ColumnBuilder::String(b) => Scalar::String(StringType::build_scalar(b)),
            ColumnBuilder::Timestamp(b) => Scalar::Timestamp(TimestampType::build_scalar(b)),
            ColumnBuilder::TimestampTz(b) => Scalar::TimestampTz(TimestampTzType::build_scalar(b)),
            ColumnBuilder::Date(b) => Scalar::Date(DateType::build_scalar(b)),
            ColumnBuilder::Interval(b) => Scalar::Interval(IntervalType::build_scalar(b)),
            ColumnBuilder::Bitmap(b) => Scalar::Bitmap(BitmapType::build_scalar(b)),
            ColumnBuilder::Variant(b) => Scalar::Variant(VariantType::build_scalar(b)),
            ColumnBuilder::Geometry(b) => Scalar::Geometry(GeometryType::build_scalar(b)),
            ColumnBuilder::Geography(b) => Scalar::Geography(GeographyType::build_scalar(b)),
            ColumnBuilder::Vector(b) => Scalar::Vector(b.build_scalar()),
            ColumnBuilder::Opaque(b) => Scalar::Opaque(b.build_scalar()),
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

unsafe impl TrustedLen for ColumnIterator<'_> {}

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
