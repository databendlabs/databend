// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance data types, [Schema] and [Field]

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, LazyLock};

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field as ArrowField, Fields, TimeUnit};
use deepsize::DeepSizeOf;
use lance_arrow::bfloat16::{is_bfloat16_field, BFLOAT16_EXT_NAME};
use lance_arrow::{ARROW_EXT_META_KEY, ARROW_EXT_NAME_KEY};
use snafu::location;

mod field;
mod schema;

use crate::{Error, Result};
pub use field::{
    BlobVersion, Encoding, Field, NullabilityComparison, OnTypeMismatch, SchemaCompareOptions,
};
pub use schema::{
    escape_field_path_for_project, format_field_path, parse_field_path, BlobHandling, FieldRef,
    OnMissing, Projectable, Projection, Schema,
};

pub static BLOB_DESC_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        ArrowField::new("position", DataType::UInt64, true),
        ArrowField::new("size", DataType::UInt64, true),
    ])
});

pub static BLOB_DESC_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(BLOB_DESC_FIELDS.clone()));

pub static BLOB_DESC_FIELD: LazyLock<ArrowField> = LazyLock::new(|| {
    ArrowField::new("description", BLOB_DESC_TYPE.clone(), true).with_metadata(HashMap::from([(
        lance_arrow::BLOB_META_KEY.to_string(),
        "true".to_string(),
    )]))
});

pub static BLOB_DESC_LANCE_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::try_from(&*BLOB_DESC_FIELD).unwrap());

pub static BLOB_V2_DESC_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        ArrowField::new("kind", DataType::UInt8, false),
        ArrowField::new("position", DataType::UInt64, true),
        ArrowField::new("size", DataType::UInt64, true),
        ArrowField::new("blob_id", DataType::UInt32, true),
        ArrowField::new("blob_uri", DataType::Utf8, true),
    ])
});

pub static BLOB_V2_DESC_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(BLOB_V2_DESC_FIELDS.clone()));

pub static BLOB_V2_DESC_FIELD: LazyLock<ArrowField> = LazyLock::new(|| {
    ArrowField::new("description", BLOB_V2_DESC_TYPE.clone(), true).with_metadata(HashMap::from([
        (lance_arrow::BLOB_META_KEY.to_string(), "true".to_string()),
    ]))
});

pub static BLOB_V2_DESC_LANCE_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::try_from(&*BLOB_V2_DESC_FIELD).unwrap());

pub const BLOB_LOGICAL_TYPE: &str = "blob";

/// LogicalType is a string presentation of arrow type.
/// to be serialized into protobuf.
#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct LogicalType(String);

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LogicalType {
    fn is_list(&self) -> bool {
        self.0 == "list" || self.0 == "list.struct"
    }

    fn is_large_list(&self) -> bool {
        self.0 == "large_list" || self.0 == "large_list.struct"
    }

    fn is_struct(&self) -> bool {
        self.0 == "struct"
    }

    fn is_blob(&self) -> bool {
        self.0 == BLOB_LOGICAL_TYPE
    }
}

impl From<&str> for LogicalType {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

fn timeunit_to_str(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn parse_timeunit(unit: &str) -> Result<TimeUnit> {
    match unit {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        _ => Err(Error::Arrow {
            message: format!("Unsupported TimeUnit: {unit}"),
            location: location!(),
        }),
    }
}

impl TryFrom<&DataType> for LogicalType {
    type Error = Error;

    fn try_from(dt: &DataType) -> Result<Self> {
        let type_str = match dt {
            DataType::Null => "null".to_string(),
            DataType::Boolean => "bool".to_string(),
            DataType::Int8 => "int8".to_string(),
            DataType::UInt8 => "uint8".to_string(),
            DataType::Int16 => "int16".to_string(),
            DataType::UInt16 => "uint16".to_string(),
            DataType::Int32 => "int32".to_string(),
            DataType::UInt32 => "uint32".to_string(),
            DataType::Int64 => "int64".to_string(),
            DataType::UInt64 => "uint64".to_string(),
            DataType::Float16 => "halffloat".to_string(),
            DataType::Float32 => "float".to_string(),
            DataType::Float64 => "double".to_string(),
            DataType::Decimal128(precision, scale) => format!("decimal:128:{precision}:{scale}"),
            DataType::Decimal256(precision, scale) => format!("decimal:256:{precision}:{scale}"),
            DataType::Utf8 => "string".to_string(),
            DataType::Binary => "binary".to_string(),
            DataType::LargeUtf8 => "large_string".to_string(),
            DataType::LargeBinary => "large_binary".to_string(),
            DataType::Date32 => "date32:day".to_string(),
            DataType::Date64 => "date64:ms".to_string(),
            DataType::Time32(tu) => format!("time32:{}", timeunit_to_str(tu)),
            DataType::Time64(tu) => format!("time64:{}", timeunit_to_str(tu)),
            DataType::Timestamp(tu, tz) => format!(
                "timestamp:{}:{}",
                timeunit_to_str(tu),
                tz.as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or("-".to_string())
            ),
            DataType::Duration(tu) => format!("duration:{}", timeunit_to_str(tu)),
            DataType::Struct(_) => "struct".to_string(),
            DataType::Dictionary(key_type, value_type) => {
                format!(
                    "dict:{}:{}:{}",
                    Self::try_from(value_type.as_ref())?.0,
                    Self::try_from(key_type.as_ref())?.0,
                    // Arrow C++ Dictionary has "ordered:bool" field, but it does not exist in `arrow-rs`.
                    false
                )
            }
            DataType::List(elem) => match elem.data_type() {
                DataType::Struct(_) => "list.struct".to_string(),
                _ => "list".to_string(),
            },
            DataType::LargeList(elem) => match elem.data_type() {
                DataType::Struct(_) => "large_list.struct".to_string(),
                _ => "large_list".to_string(),
            },
            DataType::FixedSizeList(field, len) => {
                if is_bfloat16_field(field) {
                    // Don't want to directly use `bfloat16`, in case a built-in type is added
                    // that isn't identical to our extension type.
                    format!("fixed_size_list:lance.bfloat16:{}", *len)
                } else {
                    format!(
                        "fixed_size_list:{}:{}",
                        Self::try_from(field.data_type())?.0,
                        *len
                    )
                }
            }
            DataType::FixedSizeBinary(len) => format!("fixed_size_binary:{}", *len),
            _ => {
                return Err(Error::Schema {
                    message: format!("Unsupported data type: {:?}", dt),
                    location: location!(),
                })
            }
        };

        Ok(Self(type_str))
    }
}

impl TryFrom<&LogicalType> for DataType {
    type Error = Error;

    fn try_from(lt: &LogicalType) -> Result<Self> {
        use DataType::*;
        if let Some(t) = match lt.0.as_str() {
            "null" => Some(Null),
            "bool" => Some(Boolean),
            "int8" => Some(Int8),
            "uint8" => Some(UInt8),
            "int16" => Some(Int16),
            "uint16" => Some(UInt16),
            "int32" => Some(Int32),
            "uint32" => Some(UInt32),
            "int64" => Some(Int64),
            "uint64" => Some(UInt64),
            "halffloat" => Some(Float16),
            "float" => Some(Float32),
            "double" => Some(Float64),
            "string" => Some(Utf8),
            "binary" => Some(Binary),
            "large_string" => Some(LargeUtf8),
            "large_binary" => Some(LargeBinary),
            BLOB_LOGICAL_TYPE => Some(LargeBinary),
            "json" => Some(LargeBinary),
            "date32:day" => Some(Date32),
            "date64:ms" => Some(Date64),
            "time32:s" => Some(Time32(TimeUnit::Second)),
            "time32:ms" => Some(Time32(TimeUnit::Millisecond)),
            "time64:us" => Some(Time64(TimeUnit::Microsecond)),
            "time64:ns" => Some(Time64(TimeUnit::Nanosecond)),
            "duration:s" => Some(Duration(TimeUnit::Second)),
            "duration:ms" => Some(Duration(TimeUnit::Millisecond)),
            "duration:us" => Some(Duration(TimeUnit::Microsecond)),
            "duration:ns" => Some(Duration(TimeUnit::Nanosecond)),
            _ => None,
        } {
            Ok(t)
        } else {
            let splits = lt.0.split(':').collect::<Vec<_>>();
            match splits[0] {
                "fixed_size_list" => {
                    if splits.len() < 3 {
                        return Err(Error::Schema {
                            message: format!("Unsupported logical type: {}", lt),
                            location: location!(),
                        });
                    }

                    let size: i32 =
                        splits
                            .last()
                            .unwrap()
                            .parse::<i32>()
                            .map_err(|e: _| Error::Schema {
                                message: e.to_string(),
                                location: location!(),
                            })?;

                    let inner_type = splits[1..splits.len() - 1].join(":");

                    match inner_type.as_str() {
                        BFLOAT16_EXT_NAME => {
                            let field = ArrowField::new("item", Self::FixedSizeBinary(2), true)
                                .with_metadata(
                                    [
                                        (ARROW_EXT_NAME_KEY.into(), BFLOAT16_EXT_NAME.into()),
                                        (ARROW_EXT_META_KEY.into(), "".into()),
                                    ]
                                    .into(),
                                );
                            Ok(FixedSizeList(Arc::new(field), size))
                        }
                        data_type => {
                            let elem_type = (&LogicalType(data_type.to_string())).try_into()?;

                            Ok(FixedSizeList(
                                Arc::new(ArrowField::new("item", elem_type, true)),
                                size,
                            ))
                        }
                    }
                }
                "fixed_size_binary" => {
                    if splits.len() != 2 {
                        Err(Error::Schema {
                            message: format!("Unsupported logical type: {}", lt),
                            location: location!(),
                        })
                    } else {
                        let size: i32 = splits[1].parse::<i32>().map_err(|e: _| Error::Schema {
                            message: e.to_string(),
                            location: location!(),
                        })?;
                        Ok(FixedSizeBinary(size))
                    }
                }
                "dict" => {
                    if splits.len() != 4 {
                        Err(Error::Schema {
                            message: format!("Unsupported dictionary type: {}", lt),
                            location: location!(),
                        })
                    } else {
                        let value_type: Self = (&LogicalType::from(splits[1])).try_into()?;
                        let index_type: Self = (&LogicalType::from(splits[2])).try_into()?;
                        Ok(Dictionary(Box::new(index_type), Box::new(value_type)))
                    }
                }
                "decimal" => {
                    if splits.len() != 4 {
                        Err(Error::Schema {
                            message: format!("Unsupported decimal type: {}", lt),
                            location: location!(),
                        })
                    } else {
                        let bits: i16 = splits[1].parse::<i16>().map_err(|err| Error::Schema {
                            message: err.to_string(),
                            location: location!(),
                        })?;
                        let precision: u8 =
                            splits[2].parse::<u8>().map_err(|err| Error::Schema {
                                message: err.to_string(),
                                location: location!(),
                            })?;
                        let scale: i8 = splits[3].parse::<i8>().map_err(|err| Error::Schema {
                            message: err.to_string(),
                            location: location!(),
                        })?;

                        if bits == 128 {
                            Ok(Decimal128(precision, scale))
                        } else if bits == 256 {
                            Ok(Decimal256(precision, scale))
                        } else {
                            Err(Error::Schema {
                                message: format!(
                                    "Only Decimal128 and Decimal256 is supported. Found {bits}"
                                ),
                                location: location!(),
                            })
                        }
                    }
                }
                "timestamp" => {
                    if splits.len() != 3 {
                        Err(Error::Schema {
                            message: format!("Unsupported timestamp type: {}", lt),
                            location: location!(),
                        })
                    } else {
                        let timeunit = parse_timeunit(splits[1])?;
                        let tz: Option<Arc<str>> = if splits[2] == "-" {
                            None
                        } else {
                            Some(splits[2].into())
                        };
                        Ok(Timestamp(timeunit, tz))
                    }
                }
                _ => Err(Error::Schema {
                    message: format!("Unsupported logical type: {}", lt),
                    location: location!(),
                }),
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Dictionary {
    pub offset: usize,

    pub length: usize,

    pub values: Option<ArrayRef>,
}

impl DeepSizeOf for Dictionary {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.values
            .as_ref()
            .map(|v| v.get_array_memory_size())
            .unwrap_or(0)
    }
}

impl PartialEq for Dictionary {
    fn eq(&self, other: &Self) -> bool {
        match (&self.values, &other.values) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

/// Returns true if Lance supports writing this datatype with nulls.
pub fn lance_supports_nulls(datatype: &DataType) -> bool {
    matches!(
        datatype,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::List(_)
            | DataType::FixedSizeBinary(_)
            | DataType::FixedSizeList(_, _)
    )
}
