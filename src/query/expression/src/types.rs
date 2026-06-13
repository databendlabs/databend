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

pub mod any;
pub mod array;
pub mod binary;
pub mod bitmap;
pub mod boolean;
pub mod compute_view;
pub mod date;
pub mod decimal;
pub mod empty_array;
pub mod empty_map;
pub mod generic;
pub mod geography;
pub mod geometry;
pub mod interval;
pub mod map;
pub mod null;
pub mod nullable;
pub mod number;
pub mod number_class;
pub mod opaque;
pub mod simple_type;
pub mod string;
pub mod timestamp;
pub mod timestamp_tz;
pub mod tuple;
pub mod variant;
pub mod vector;
pub mod zero_size_type;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::TrustedLen;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Range;

use databend_common_ast::ast::TypeName;
pub use databend_common_base::base::OrderedFloat;
use databend_common_exception::ErrorCode;
pub use databend_common_io::deserialize_bitmap;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

pub use self::OpaqueColumn;
pub use self::OpaqueColumnBuilder;
pub use self::any::AnyType;
pub use self::array::ArrayColumn;
pub use self::array::ArrayType;
pub use self::binary::BinaryColumn;
pub use self::binary::BinaryType;
pub use self::bitmap::BitmapType;
pub use self::boolean::Bitmap;
pub use self::boolean::BooleanType;
pub use self::boolean::MutableBitmap;
pub use self::date::DateType;
pub use self::decimal::*;
pub use self::empty_array::EmptyArrayType;
pub use self::empty_map::EmptyMapType;
pub use self::generic::GenericType;
pub use self::geography::GeographyColumn;
pub use self::geography::GeographyType;
pub use self::geometry::GeometryType;
pub use self::interval::IntervalType;
pub use self::map::MapType;
pub use self::null::NullType;
pub use self::nullable::NullableColumn;
pub use self::nullable::NullableType;
pub use self::number::*;
pub use self::number_class::*;
pub use self::opaque::*;
use self::simple_type::*;
pub use self::string::StringColumn;
pub use self::string::StringType;
pub use self::timestamp::TimestampType;
pub use self::timestamp_tz::TimestampTzType;
pub use self::tuple::*;
pub use self::variant::VariantType;
pub use self::vector::VectorColumn;
pub use self::vector::VectorColumnBuilder;
pub use self::vector::VectorColumnVec;
pub use self::vector::VectorDataType;
pub use self::vector::VectorScalar;
pub use self::vector::VectorScalarRef;
pub use self::vector::VectorType;
use self::zero_size_type::*;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::property::Domain;
use crate::types::date::DATE_MAX;
use crate::types::date::DATE_MIN;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::values::Column;
use crate::values::Scalar;

pub type GenericMap = [DataType];

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum DataType {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean,
    Binary,
    String,
    Number(NumberDataType),
    Decimal(DecimalSize),
    Timestamp,
    TimestampTz,
    Date,
    Nullable(Box<DataType>),
    Array(Box<DataType>),
    Map(Box<DataType>),
    Bitmap,
    Tuple(Vec<DataType>),
    Variant,
    Geometry,
    Interval,
    Geography,
    Vector(VectorDataType),
    Opaque(usize),

    // Used internally for generic types
    Generic(usize),

    StageLocation,
}

impl DataType {
    pub fn wrap_nullable(&self) -> Self {
        match self {
            DataType::Null | DataType::Nullable(_) => self.clone(),
            _ => Self::Nullable(Box::new(self.clone())),
        }
    }

    pub fn nest_wrap_nullable(&self) -> Self {
        match self {
            DataType::Null => self.clone(),
            DataType::Nullable(box inner_ty) => inner_ty.nest_wrap_nullable(),
            DataType::Array(box inner_ty) => Self::Nullable(Box::new(Self::Array(Box::new(
                inner_ty.nest_wrap_nullable(),
            )))),
            DataType::Map(box DataType::Tuple(inner_tys)) if inner_tys.len() == 2 => {
                let key_ty = inner_tys[0].clone();
                let val_ty = inner_tys[1].nest_wrap_nullable();
                Self::Nullable(Box::new(Self::Map(Box::new(Self::Tuple(vec![
                    key_ty, val_ty,
                ])))))
            }
            DataType::Tuple(inner_tys) => {
                let new_inner_tys = inner_tys
                    .iter()
                    .map(|inner_ty| inner_ty.nest_wrap_nullable())
                    .collect();
                Self::Nullable(Box::new(Self::Tuple(new_inner_tys)))
            }
            _ => Self::Nullable(Box::new(self.clone())),
        }
    }

    pub fn is_nullable_or_null(&self) -> bool {
        matches!(self, &DataType::Nullable(_) | &DataType::Null)
    }

    pub fn can_inside_nullable(&self) -> bool {
        !self.is_nullable_or_null()
    }

    pub fn remove_nullable(&self) -> Self {
        match self {
            DataType::Nullable(ty) => (**ty).clone(),
            _ => self.clone(),
        }
    }

    pub fn unnest(&self) -> Self {
        match self {
            DataType::Array(ty) => ty.unnest(),
            _ => self.clone(),
        }
    }

    pub fn has_generic(&self) -> bool {
        match self {
            DataType::Generic(_) => true,
            DataType::Null
            | DataType::EmptyArray
            | DataType::EmptyMap
            | DataType::Boolean
            | DataType::Binary
            | DataType::String
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date
            | DataType::Interval
            | DataType::Bitmap
            | DataType::Variant
            | DataType::Geometry
            | DataType::Geography
            | DataType::Vector(_)
            | DataType::Opaque(_)
            | DataType::StageLocation => false,
            DataType::Nullable(ty) => ty.has_generic(),
            DataType::Array(ty) => ty.has_generic(),
            DataType::Map(ty) => ty.has_generic(),
            DataType::Tuple(tys) => tys.iter().any(|ty| ty.has_generic()),
        }
    }

    pub fn remove_generics(&self, generics: &[DataType]) -> DataType {
        match self {
            DataType::Generic(i) => generics[*i].clone(),
            DataType::Null
            | DataType::EmptyArray
            | DataType::EmptyMap
            | DataType::Boolean
            | DataType::Binary
            | DataType::String
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date
            | DataType::Interval
            | DataType::Bitmap
            | DataType::Variant
            | DataType::Geometry
            | DataType::Geography
            | DataType::Vector(_)
            | DataType::Opaque(_)
            | DataType::StageLocation => self.clone(),
            DataType::Nullable(ty) => DataType::Nullable(Box::new(ty.remove_generics(generics))),
            DataType::Array(ty) => DataType::Array(Box::new(ty.remove_generics(generics))),
            DataType::Map(ty) => DataType::Map(Box::new(ty.remove_generics(generics))),
            DataType::Tuple(tys) => {
                DataType::Tuple(tys.iter().map(|ty| ty.remove_generics(generics)).collect())
            }
        }
    }

    pub fn has_nested_nullable(&self) -> bool {
        match self {
            DataType::Null
            | DataType::EmptyArray
            | DataType::EmptyMap
            | DataType::Boolean
            | DataType::Binary
            | DataType::String
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date
            | DataType::Interval
            | DataType::Bitmap
            | DataType::Variant
            | DataType::Geometry
            | DataType::Geography
            | DataType::Vector(_)
            | DataType::Generic(_) => false,
            DataType::Nullable(box DataType::Nullable(_) | box DataType::Null) => true,
            DataType::Nullable(ty) => ty.has_nested_nullable(),
            DataType::Array(ty) => ty.has_nested_nullable(),
            DataType::Map(ty) => ty.has_nested_nullable(),
            DataType::Tuple(tys) => tys.iter().any(|ty| ty.has_nested_nullable()),
            DataType::Opaque(_) => false,
            DataType::StageLocation => false,
        }
    }

    pub fn infinity(&self) -> Result<Scalar, String> {
        match &self {
            DataType::Timestamp => Ok(Scalar::Timestamp(TIMESTAMP_MAX)),
            DataType::Date => Ok(Scalar::Date(DATE_MAX)),
            DataType::Number(NumberDataType::Float32) => Ok(Scalar::Number(NumberScalar::Float32(
                OrderedFloat(f32::INFINITY),
            ))),
            DataType::Number(NumberDataType::Int32) => {
                Ok(Scalar::Number(NumberScalar::Int32(i32::MAX)))
            }
            DataType::Number(NumberDataType::Int16) => {
                Ok(Scalar::Number(NumberScalar::Int16(i16::MAX)))
            }
            DataType::Number(NumberDataType::Int8) => {
                Ok(Scalar::Number(NumberScalar::Int8(i8::MAX)))
            }
            DataType::Number(NumberDataType::Float64) => Ok(Scalar::Number(NumberScalar::Float64(
                OrderedFloat(f64::INFINITY),
            ))),
            DataType::Number(NumberDataType::Int64) => {
                Ok(Scalar::Number(NumberScalar::Int64(i64::MAX)))
            }
            _ => Err(format!(
                "only support numeric types and time types, but got {:?}",
                self
            )),
        }
    }
    pub fn ninfinity(&self) -> Result<Scalar, String> {
        match &self {
            DataType::Timestamp => Ok(Scalar::Timestamp(TIMESTAMP_MIN)),
            DataType::Date => Ok(Scalar::Date(DATE_MIN)),
            DataType::Number(NumberDataType::Float32) => Ok(Scalar::Number(NumberScalar::Float32(
                OrderedFloat(f32::NEG_INFINITY),
            ))),
            DataType::Number(NumberDataType::Int32) => {
                Ok(Scalar::Number(NumberScalar::Int32(i32::MIN)))
            }
            DataType::Number(NumberDataType::Int16) => {
                Ok(Scalar::Number(NumberScalar::Int16(i16::MIN)))
            }
            DataType::Number(NumberDataType::Int8) => {
                Ok(Scalar::Number(NumberScalar::Int8(i8::MIN)))
            }
            DataType::Number(NumberDataType::Float64) => Ok(Scalar::Number(NumberScalar::Float64(
                OrderedFloat(f64::NEG_INFINITY),
            ))),
            DataType::Number(NumberDataType::Int64) => {
                Ok(Scalar::Number(NumberScalar::Int64(i64::MIN)))
            }
            _ => Err(format!(
                "only support numeric types and time types, but got {:?}",
                self
            )),
        }
    }

    pub fn is_unsigned_numeric(&self) -> bool {
        match self {
            DataType::Number(ty) => ALL_UNSIGNED_INTEGER_TYPES.contains(ty),
            _ => false,
        }
    }

    pub fn is_signed_numeric(&self) -> bool {
        match self {
            DataType::Number(ty) => {
                ALL_INTEGER_TYPES.contains(ty) && !ALL_UNSIGNED_INTEGER_TYPES.contains(ty)
            }
            _ => false,
        }
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        match self {
            DataType::Number(ty) => ALL_INTEGER_TYPES.contains(ty),
            _ => false,
        }
    }

    #[inline]
    pub fn is_floating(&self) -> bool {
        match self {
            DataType::Number(ty) => ALL_FLOAT_TYPES.contains(ty),
            _ => false,
        }
    }

    #[inline]
    pub fn is_date_or_date_time(&self) -> bool {
        matches!(self, DataType::Timestamp | DataType::Date)
    }

    #[inline]
    pub fn is_string_column(&self) -> bool {
        match self {
            DataType::Binary | DataType::String | DataType::Bitmap | DataType::Variant => true,
            DataType::Nullable(ty) => ty.is_string_column(),
            _ => false,
        }
    }

    pub fn numeric_byte_size(&self) -> Option<usize> {
        match self {
            DataType::Number(number) => {
                use NumberDataType::*;
                let n = match number {
                    UInt8 | Int8 => 1,
                    UInt16 | Int16 => 2,
                    UInt32 | Int32 | Float32 => 4,
                    UInt64 | Int64 | Float64 => 8,
                };
                Some(n)
            }
            DataType::Date => Some(4),
            DataType::Timestamp => Some(8),
            DataType::Interval => None, // todo
            DataType::Decimal(size) => {
                let n = if size.can_carried_by_64() {
                    8
                } else if size.can_carried_by_128() {
                    16
                } else {
                    32
                };
                Some(n)
            }
            _ => None,
        }
    }

    // Nullable will be displayed as Nullable(T)
    pub fn wrapped_display(&self) -> String {
        match self {
            DataType::Nullable(inner_ty) => format!("Nullable({})", inner_ty.wrapped_display()),
            DataType::TimestampTz => "Timestamp_Tz".to_string(),
            _ => format!("{}", self),
        }
    }

    pub fn sql_name(&self) -> String {
        match self {
            DataType::Number(num_ty) => match num_ty {
                NumberDataType::UInt8 => "TINYINT UNSIGNED",
                NumberDataType::UInt16 => "SMALLINT UNSIGNED",
                NumberDataType::UInt32 => "INT UNSIGNED",
                NumberDataType::UInt64 => "BIGINT UNSIGNED",
                NumberDataType::Int8 => "TINYINT",
                NumberDataType::Int16 => "SMALLINT",
                NumberDataType::Int32 => "INT",
                NumberDataType::Int64 => "BIGINT",
                NumberDataType::Float32 => "FLOAT",
                NumberDataType::Float64 => "DOUBLE",
            }
            .to_string(),
            DataType::String => "VARCHAR".to_string(),
            DataType::Nullable(inner_ty) => format!("{} NULL", inner_ty.sql_name()),
            DataType::TimestampTz => "TIMESTAMP_TZ".to_string(),
            _ => self.to_string().to_uppercase(),
        }
    }

    pub fn to_type_name(&self) -> databend_common_exception::Result<TypeName> {
        match self {
            DataType::Number(num_ty) => match num_ty {
                NumberDataType::UInt8 => Ok(TypeName::UInt8),
                NumberDataType::UInt16 => Ok(TypeName::UInt16),
                NumberDataType::UInt32 => Ok(TypeName::UInt32),
                NumberDataType::UInt64 => Ok(TypeName::UInt64),
                NumberDataType::Int8 => Ok(TypeName::Int8),
                NumberDataType::Int16 => Ok(TypeName::Int16),
                NumberDataType::Int32 => Ok(TypeName::Int32),
                NumberDataType::Int64 => Ok(TypeName::Int64),
                NumberDataType::Float32 => Ok(TypeName::Float32),
                NumberDataType::Float64 => Ok(TypeName::Float64),
            },
            DataType::String => Ok(TypeName::String),
            DataType::Date => Ok(TypeName::Date),
            DataType::Timestamp => Ok(TypeName::Timestamp),
            DataType::TimestampTz => Ok(TypeName::TimestampTz),
            DataType::Interval => Ok(TypeName::Interval),
            DataType::Decimal(size) => {
                let precision = size.precision();
                let scale = size.scale();
                Ok(TypeName::Decimal { precision, scale })
            }
            DataType::Array(inner_ty) => {
                let inner_ty = inner_ty.to_type_name()?;
                Ok(TypeName::Array(Box::new(inner_ty)))
            }
            DataType::Map(inner_ty) => {
                let inner_ty = inner_ty.as_tuple().unwrap();
                let key_ty = inner_ty[0].to_type_name()?;
                let val_ty = inner_ty[1].to_type_name()?;
                Ok(TypeName::Map {
                    key_type: Box::new(key_ty),
                    val_type: Box::new(val_ty),
                })
            }
            DataType::Bitmap => Ok(TypeName::Bitmap),
            DataType::Variant => Ok(TypeName::Variant),
            DataType::Geometry => Ok(TypeName::Geometry),
            DataType::Geography => Ok(TypeName::Geography),
            DataType::Tuple(inner_tys) => {
                let inner_tys = inner_tys
                    .iter()
                    .map(|inner_ty| inner_ty.to_type_name())
                    .collect::<Result<Vec<TypeName>, ErrorCode>>()?;
                Ok(TypeName::Tuple {
                    fields_name: None,
                    fields_type: inner_tys,
                })
            }
            DataType::Vector(inner_ty) => {
                let d = inner_ty.dimension();
                Ok(TypeName::Vector(d))
            }
            DataType::Nullable(inner_ty) => {
                Ok(TypeName::Nullable(Box::new(inner_ty.to_type_name()?)))
            }
            DataType::Boolean => Ok(TypeName::Boolean),
            DataType::Binary => Ok(TypeName::Binary),
            DataType::Null
            | DataType::EmptyArray
            | DataType::EmptyMap
            | DataType::Opaque(_)
            | DataType::Generic(_)
            | DataType::StageLocation => Err(ErrorCode::BadArguments(format!(
                "Unsupported data type {} to sql type",
                self
            ))),
        }
    }

    // Returns the number of leaf columns of the DataType
    pub fn num_leaf_columns(&self) -> usize {
        match self {
            DataType::Nullable(box inner_ty)
            | DataType::Array(box inner_ty)
            | DataType::Map(box inner_ty) => inner_ty.num_leaf_columns(),
            DataType::Tuple(inner_tys) => inner_tys
                .iter()
                .map(|inner_ty| inner_ty.num_leaf_columns())
                .sum(),
            _ => 1,
        }
    }

    pub fn get_decimal_properties(&self) -> Option<DecimalSize> {
        match self {
            DataType::Decimal(size) => Some(*size),
            DataType::Number(num_ty) => num_ty.get_decimal_properties(),
            _ => None,
        }
    }

    pub fn is_physical_binary(&self) -> bool {
        matches!(
            self,
            DataType::Binary
                | DataType::Bitmap
                | DataType::Variant
                | DataType::Geometry
                | DataType::Geography
        )
    }
}

pub fn convert_to_type_name(ty: &DataType) -> TypeName {
    match ty {
        DataType::Boolean => TypeName::Boolean,
        DataType::Number(NumberDataType::UInt8) => TypeName::UInt8,
        DataType::Number(NumberDataType::UInt16) => TypeName::UInt16,
        DataType::Number(NumberDataType::UInt32) => TypeName::UInt32,
        DataType::Number(NumberDataType::UInt64) => TypeName::UInt64,
        DataType::Number(NumberDataType::Int8) => TypeName::Int8,
        DataType::Number(NumberDataType::Int16) => TypeName::Int16,
        DataType::Number(NumberDataType::Int32) => TypeName::Int32,
        DataType::Number(NumberDataType::Int64) => TypeName::Int64,
        DataType::Number(NumberDataType::Float32) => TypeName::Float32,
        DataType::Number(NumberDataType::Float64) => TypeName::Float64,
        DataType::Decimal(size) => TypeName::Decimal {
            precision: size.precision(),
            scale: size.scale(),
        },
        DataType::Date => TypeName::Date,
        DataType::Timestamp => TypeName::Timestamp,
        DataType::String => TypeName::String,
        DataType::Bitmap => TypeName::Bitmap,
        DataType::Variant => TypeName::Variant,
        DataType::Binary => TypeName::Binary,
        DataType::Geometry => TypeName::Geometry,
        DataType::Nullable(box inner_ty) => {
            TypeName::Nullable(Box::new(convert_to_type_name(inner_ty)))
        }
        DataType::Array(box inner_ty) => TypeName::Array(Box::new(convert_to_type_name(inner_ty))),
        DataType::Map(box inner_ty) => match inner_ty {
            DataType::Tuple(inner_tys) => TypeName::Map {
                key_type: Box::new(convert_to_type_name(&inner_tys[0])),
                val_type: Box::new(convert_to_type_name(&inner_tys[1])),
            },
            _ => unreachable!(),
        },
        DataType::Tuple(inner_tys) => TypeName::Tuple {
            fields_name: None,
            fields_type: inner_tys
                .iter()
                .map(convert_to_type_name)
                .collect::<Vec<_>>(),
        },
        _ => TypeName::String,
    }
}

/// [AccessType] defines a series of access methods for a data type
pub trait AccessType: Debug + Clone + PartialEq + Sized + 'static {
    type Scalar: Debug + Clone + PartialEq + Send + 'static;
    type ScalarRef<'a>: Debug + Clone + PartialEq;
    type Column: Debug + Clone + PartialEq + Send;
    type Domain: Debug + Clone + PartialEq;
    type ColumnIterator<'a>: Iterator<Item = Self::ScalarRef<'a>> + TrustedLen;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar;
    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_>;

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>, ErrorCode>;
    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain, ErrorCode>;

    fn try_downcast_column(col: &Column) -> Result<Self::Column, ErrorCode>;

    fn column_len(col: &Self::Column) -> usize;
    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>>;

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_>;

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    unsafe fn index_column_unchecked_scalar(col: &Self::Column, index: usize) -> Self::Scalar {
        unsafe { Self::to_owned_scalar(Self::index_column_unchecked(col, index)) }
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column;
    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_>;

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        std::mem::size_of::<Self::Scalar>()
    }

    fn column_memory_size(col: &Self::Column, gc: bool) -> usize {
        let _ = gc;
        Self::column_len(col) * std::mem::size_of::<Self::Scalar>()
    }

    /// Compare two scalar values.
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering;

    /// Equal comparison between two scalars, some data types not support comparison.
    #[inline(always)]
    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        matches!(Self::compare(left, right), Ordering::Equal)
    }

    /// Not equal comparison between two scalars, some data types not support comparison.
    #[inline(always)]
    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !matches!(Self::compare(left, right), Ordering::Equal)
    }

    /// Greater than comparison between two scalars, some data types not support comparison.
    #[inline(always)]
    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        matches!(Self::compare(left, right), Ordering::Greater)
    }

    /// Less than comparison between two scalars, some data types not support comparison.
    #[inline(always)]
    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        matches!(Self::compare(left, right), Ordering::Less)
    }

    /// Greater than or equal comparison between two scalars, some data types not support comparison.
    #[inline(always)]
    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !matches!(Self::compare(left, right), Ordering::Less)
    }

    /// Less than or equal comparison between two scalars, some data types not support comparison.
    #[inline(always)]
    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !matches!(Self::compare(left, right), Ordering::Greater)
    }
}

fn scalar_type_error<T>(value: &ScalarRef<'_>) -> ErrorCode {
    ErrorCode::BadDataValueType(format!(
        "failed to downcast scalar {:?} into {}",
        value,
        std::any::type_name::<T>()
    ))
}

fn column_type_error<T>(value: &Column) -> ErrorCode {
    ErrorCode::BadDataValueType(format!(
        "failed to downcast column {:?} into {}",
        value,
        std::any::type_name::<T>()
    ))
}

fn domain_type_error<T>(value: &Domain) -> ErrorCode {
    ErrorCode::BadDataValueType(format!(
        "failed to downcast domain {:?} into {}",
        value,
        std::any::type_name::<T>()
    ))
}

/// [ValueType] includes the builder method of a data type based on [AccessType].
pub trait ValueType: AccessType {
    type ColumnBuilder: Debug + Clone;
    type ColumnBuilderMut<'a>: Debug + From<&'a mut Self::ColumnBuilder> + BuilderExt<Self>;

    /// Convert a scalar value to the generic [Scalar] type
    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar;

    /// Convert a domain value to the generic Domain type
    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain;

    /// Convert a column value to the generic Column type
    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column;

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_>;

    // removed by #18102, but there may be a need for this somewhere
    // fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder>;

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder>;

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder;

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize;
    fn builder_len(builder: &Self::ColumnBuilder) -> usize;

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>);
    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        Self::push_item_mut(&mut builder.into(), item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    );
    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        Self::push_item_repeat_mut(&mut builder.into(), item, n);
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>);
    fn push_default(builder: &mut Self::ColumnBuilder) {
        Self::push_default_mut(&mut builder.into())
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column);
    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        Self::append_column_mut(&mut builder.into(), other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column;
    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar;
}

impl<'a, T: ValueType> From<&'a mut T::ColumnBuilder> for BuilderMut<'a, T> {
    fn from(value: &'a mut T::ColumnBuilder) -> Self {
        BuilderMut(value)
    }
}

#[derive(Debug)]
pub struct BuilderMut<'a, T: ValueType>(&'a mut T::ColumnBuilder);

impl<T: ValueType> Deref for BuilderMut<'_, T> {
    type Target = T::ColumnBuilder;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<T: ValueType> DerefMut for BuilderMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'a, T> BuilderExt<T> for BuilderMut<'a, T>
where T: ValueType<ColumnBuilderMut<'a> = Self>
{
    fn len(&self) -> usize {
        T::builder_len(self.0)
    }

    fn push_item(&mut self, item: <T>::ScalarRef<'_>) {
        T::push_item_mut(self, item);
    }

    fn push_repeat(&mut self, item: <T>::ScalarRef<'_>, n: usize) {
        T::push_item_repeat_mut(self, item, n);
    }

    fn push_default(&mut self) {
        T::push_default_mut(self);
    }

    fn append_column(&mut self, other: &<T>::Column) {
        T::append_column_mut(self, other);
    }
}

pub trait BuilderExt<T: ValueType> {
    fn len(&self) -> usize;
    fn push_item(&mut self, item: T::ScalarRef<'_>);
    fn push_repeat(&mut self, item: T::ScalarRef<'_>, n: usize);
    fn push_default(&mut self);
    fn append_column(&mut self, other: &T::Column);
}

/// Almost all [ValueType] implement [ReturnType], except [AnyType].
pub trait ReturnType: ValueType {
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder;

    fn column_from_vec(vec: Vec<Self::Scalar>, generics: &GenericMap) -> Self::Column {
        Self::column_from_iter(vec.iter().cloned(), generics)
    }

    fn column_from_iter(
        iter: impl Iterator<Item = Self::Scalar>,
        generics: &GenericMap,
    ) -> Self::Column {
        let mut builder = Self::create_builder(iter.size_hint().0, generics);
        for item in iter {
            Self::push_item(&mut builder, Self::to_scalar_ref(&item));
        }
        Self::build_column(builder)
    }

    fn full_domain_with_type(data_type: &DataType) -> Self::Domain {
        let domain = Domain::full(data_type);
        Self::try_downcast_domain(&domain).unwrap()
    }
}

/// The [DataType] of [ArgType] is a unit type, so we can omit [DataType].
pub trait ArgType: ReturnType {
    fn data_type() -> DataType;
    fn full_domain() -> Self::Domain;

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Self::upcast_scalar_with_type(scalar, &Self::data_type())
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Self::upcast_domain_with_type(domain, &Self::data_type())
    }

    fn upcast_column(col: Self::Column) -> Column {
        Self::upcast_column_with_type(col, &Self::data_type())
    }
}
