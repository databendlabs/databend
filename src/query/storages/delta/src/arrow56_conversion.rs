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

//! Conversions from Delta kernel types to Arrow v56 types.
//! Adapted from https://github.com/delta-io/delta-kernel-rs/blob/v0.10.0/kernel/src/engine/arrow_conversion.rs.
use std::fmt;
use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_schema::TimeUnit;
use deltalake::kernel::error::Error;
use deltalake::kernel::ArrayType;
use deltalake::kernel::DataType;
use deltalake::kernel::MapType;
use deltalake::kernel::MetadataValue;
use deltalake::kernel::PrimitiveType;
use deltalake::kernel::StructField;
use deltalake::kernel::StructType;
use itertools::Itertools;

pub(crate) const LIST_ARRAY_ROOT: &str = "element";
pub(crate) const MAP_ROOT_DEFAULT: &str = "key_value";
pub(crate) const MAP_KEY_DEFAULT: &str = "key";
pub(crate) const MAP_VALUE_DEFAULT: &str = "value";

#[derive(Debug)]
pub(crate) enum Arrow56ConversionError {
    Arrow(deltalake::arrow::error::ArrowError),
    DeltaKernel(Error),
    SerdeJson(serde_json::Error),
    InvalidDataType(String),
}

impl fmt::Display for Arrow56ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Arrow56ConversionError::Arrow(err) => write!(f, "{err}"),
            Arrow56ConversionError::DeltaKernel(err) => write!(f, "{err}"),
            Arrow56ConversionError::SerdeJson(err) => write!(f, "{err}"),
            Arrow56ConversionError::InvalidDataType(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Arrow56ConversionError {}

impl From<deltalake::arrow::error::ArrowError> for Arrow56ConversionError {
    fn from(value: deltalake::arrow::error::ArrowError) -> Self {
        Self::Arrow(value)
    }
}

impl From<serde_json::Error> for Arrow56ConversionError {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(value)
    }
}

impl From<Error> for Arrow56ConversionError {
    fn from(value: Error) -> Self {
        Self::DeltaKernel(value)
    }
}

pub(crate) type Result<T> = std::result::Result<T, Arrow56ConversionError>;

pub(crate) trait TryFromValue<T>: Sized {
    fn try_from_value(value: T) -> Result<Self>;
}

pub(crate) trait TryIntoValue<T>: Sized {
    fn try_into_value(self) -> Result<T>;
}

impl<T, U> TryIntoValue<U> for T
where U: TryFromValue<T>
{
    #[inline]
    fn try_into_value(self) -> Result<U> {
        U::try_from_value(self)
    }
}

impl TryFromValue<&StructType> for ArrowSchema {
    fn try_from_value(s: &StructType) -> Result<Self> {
        let fields: Vec<ArrowField> = s.fields().map(TryIntoValue::try_into_value).try_collect()?;
        Ok(ArrowSchema::new(fields))
    }
}

impl TryFromValue<&StructField> for ArrowField {
    fn try_from_value(f: &StructField) -> Result<Self> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| match &val {
                &MetadataValue::String(val) => Ok((key.clone(), val.clone())),
                _ => Ok((key.clone(), serde_json::to_string(val)?)),
            })
            .collect::<std::result::Result<_, serde_json::Error>>()?;

        let field = ArrowField::new(
            f.name(),
            ArrowDataType::try_from_value(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFromValue<&ArrayType> for ArrowField {
    fn try_from_value(a: &ArrayType) -> Result<Self> {
        Ok(ArrowField::new(
            LIST_ARRAY_ROOT,
            ArrowDataType::try_from_value(a.element_type())?,
            a.contains_null(),
        ))
    }
}

impl TryFromValue<&MapType> for ArrowField {
    fn try_from_value(a: &MapType) -> Result<Self> {
        Ok(ArrowField::new(
            MAP_ROOT_DEFAULT,
            ArrowDataType::Struct(
                vec![
                    ArrowField::new(
                        MAP_KEY_DEFAULT,
                        ArrowDataType::try_from_value(a.key_type())?,
                        false,
                    ),
                    ArrowField::new(
                        MAP_VALUE_DEFAULT,
                        ArrowDataType::try_from_value(a.value_type())?,
                        a.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false, // always non-null
        ))
    }
}

impl TryFromValue<&DataType> for ArrowDataType {
    fn try_from_value(t: &DataType) -> Result<Self> {
        match t {
            DataType::Primitive(p) => {
                match p {
                    PrimitiveType::String => Ok(ArrowDataType::Utf8),
                    PrimitiveType::Long => Ok(ArrowDataType::Int64), // undocumented type
                    PrimitiveType::Integer => Ok(ArrowDataType::Int32),
                    PrimitiveType::Short => Ok(ArrowDataType::Int16),
                    PrimitiveType::Byte => Ok(ArrowDataType::Int8),
                    PrimitiveType::Float => Ok(ArrowDataType::Float32),
                    PrimitiveType::Double => Ok(ArrowDataType::Float64),
                    PrimitiveType::Boolean => Ok(ArrowDataType::Boolean),
                    PrimitiveType::Binary => Ok(ArrowDataType::Binary),
                    PrimitiveType::Decimal(dtype) => Ok(ArrowDataType::Decimal128(
                        dtype.precision(),
                        dtype.scale() as i8, // 0..=38
                    )),
                    PrimitiveType::Date => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone. Stored as 4 bytes integer representing days since 1970-01-01
                        Ok(ArrowDataType::Date32)
                    }
                    // TODO: https://github.com/delta-io/delta/issues/643
                    PrimitiveType::Timestamp => Ok(ArrowDataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("UTC".into()),
                    )),
                    PrimitiveType::TimestampNtz => {
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                }
            }
            DataType::Struct(s) => Ok(ArrowDataType::Struct(
                s.fields()
                    .map(TryIntoValue::try_into_value)
                    .collect::<Result<Vec<ArrowField>>>()?
                    .into(),
            )),
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into_value()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(
                Arc::new(m.as_ref().try_into_value()?),
                false,
            )),
        }
    }
}

impl TryFromValue<&ArrowSchema> for StructType {
    fn try_from_value(arrow_schema: &ArrowSchema) -> Result<Self> {
        StructType::try_new(
            arrow_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().try_into_value()),
        )
    }
}

impl TryFromValue<ArrowSchemaRef> for StructType {
    fn try_from_value(arrow_schema: ArrowSchemaRef) -> Result<Self> {
        arrow_schema.as_ref().try_into_value()
    }
}

impl TryFromValue<&ArrowField> for StructField {
    fn try_from_value(arrow_field: &ArrowField) -> Result<Self> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from_value(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(arrow_field.metadata().iter().map(|(k, v)| (k.clone(), v))))
    }
}

impl TryFromValue<&ArrowDataType> for DataType {
    fn try_from_value(arrow_datatype: &ArrowDataType) -> Result<Self> {
        match arrow_datatype {
            ArrowDataType::Utf8 => Ok(DataType::STRING),
            ArrowDataType::LargeUtf8 => Ok(DataType::STRING),
            ArrowDataType::Utf8View => Ok(DataType::STRING),
            ArrowDataType::Int64 => Ok(DataType::LONG), // undocumented type
            ArrowDataType::Int32 => Ok(DataType::INTEGER),
            ArrowDataType::Int16 => Ok(DataType::SHORT),
            ArrowDataType::Int8 => Ok(DataType::BYTE),
            ArrowDataType::UInt64 => Ok(DataType::LONG), // undocumented type
            ArrowDataType::UInt32 => Ok(DataType::INTEGER),
            ArrowDataType::UInt16 => Ok(DataType::SHORT),
            ArrowDataType::UInt8 => Ok(DataType::BYTE),
            ArrowDataType::Float32 => Ok(DataType::FLOAT),
            ArrowDataType::Float64 => Ok(DataType::DOUBLE),
            ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
            ArrowDataType::Binary => Ok(DataType::BINARY),
            ArrowDataType::FixedSizeBinary(_) => Ok(DataType::BINARY),
            ArrowDataType::LargeBinary => Ok(DataType::BINARY),
            ArrowDataType::BinaryView => Ok(DataType::BINARY),
            ArrowDataType::Decimal128(p, s) => {
                if *s < 0 {
                    return Err(Arrow56ConversionError::from(Error::Generic(
                        "Negative scales are not supported in Delta".to_owned(),
                    )));
                };
                DataType::decimal(*p, *s as u8)
                    .map_err(|e| Arrow56ConversionError::InvalidDataType(e.to_string()))
            }
            ArrowDataType::Date32 => Ok(DataType::DATE),
            ArrowDataType::Date64 => Ok(DataType::DATE),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::TIMESTAMP)
            }
            ArrowDataType::Struct(fields) => DataType::try_struct_type(
                fields.iter().map(|field| field.as_ref().try_into_value()),
            ),
            ArrowDataType::List(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::ListView(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::LargeList(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::LargeListView(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::FixedSizeList(field, _) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = DataType::try_from_value(struct_fields[0].data_type())?;
                    let value_type = DataType::try_from_value(struct_fields[1].data_type())?;
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(MapType::new(key_type, value_type, value_type_nullable).into())
                } else {
                    panic!("DataType::Map should contain a struct field child");
                }
            }
            // Dictionary types are just an optimized in-memory representation of an array.
            // Schema-wise, they are the same as the value type.
            ArrowDataType::Dictionary(_, value_type) => Ok(value_type.as_ref().try_into_value()?),
            s => Err(Arrow56ConversionError::InvalidDataType(format!(
                "Invalid data type for Delta Lake: {s}"
            ))),
        }
    }
}
