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

use std::collections::BTreeMap;

use databend_common_expression::ColumnId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
pub struct TableSchema {
    pub(crate) fields: Vec<TableField>,
    pub(crate) metadata: BTreeMap<String, String>,
    // next column id that assign to TableField.column_id
    #[serde(default = "uninit_column_id")]
    pub(crate) next_column_id: ColumnId,
}

fn uninit_column_id() -> ColumnId {
    0
}

#[derive(Serialize, Deserialize)]
pub struct TableField {
    name: String,
    default_expr: Option<String>,
    data_type: TableDataType,
    #[serde(default = "uninit_column_id")]
    column_id: ColumnId,
}

#[derive(Serialize, Deserialize)]
pub enum TableDataType {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean,
    String,
    Number(NumberDataType),
    Decimal(DecimalDataType),
    Timestamp,
    Date,
    Nullable(Box<TableDataType>),
    Array(Box<TableDataType>),
    Map(Box<TableDataType>),
    Bitmap,
    Tuple {
        fields_name: Vec<String>,
        fields_type: Vec<TableDataType>,
    },
    Variant,
}

#[derive(Serialize, Deserialize)]
pub enum NumberDataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

#[derive(Serialize, Deserialize)]
pub enum DecimalDataType {
    Decimal128(DecimalSize),
    Decimal256(DecimalSize),
}

#[derive(Serialize, Deserialize)]
pub struct DecimalSize {
    pub precision: u8,
    pub scale: u8,
}

// thank u, copilot
mod converters {
    use super::*;

    impl From<TableField> for databend_common_expression::TableField {
        fn from(value: TableField) -> Self {
            Self {
                name: value.name,
                default_expr: value.default_expr,
                data_type: value.data_type.into(),
                column_id: value.column_id,
                computed_expr: None,
            }
        }
    }

    impl From<TableDataType> for databend_common_expression::TableDataType {
        fn from(value: TableDataType) -> Self {
            match value {
                TableDataType::Null => Self::Null,
                TableDataType::EmptyArray => Self::EmptyArray,
                TableDataType::EmptyMap => Self::EmptyMap,
                TableDataType::Boolean => Self::Boolean,
                TableDataType::String => Self::String,
                TableDataType::Number(value) => Self::Number(value.into()),
                TableDataType::Decimal(value) => Self::Decimal(value.into()),
                TableDataType::Timestamp => Self::Timestamp,
                TableDataType::Date => Self::Date,
                TableDataType::Nullable(value) => {
                    let v = Box::<TableDataType>::into_inner(value);
                    Self::Nullable(Box::new(v.into()))
                }
                TableDataType::Array(value) => {
                    let v = Box::<TableDataType>::into_inner(value);
                    Self::Array(Box::new(v.into()))
                }
                TableDataType::Map(value) => {
                    let v = Box::<TableDataType>::into_inner(value);
                    Self::Map(Box::new(v.into()))
                }
                TableDataType::Bitmap => Self::Bitmap,
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                } => Self::Tuple {
                    fields_name,
                    fields_type: fields_type.into_iter().map(|x| x.into()).collect(),
                },
                TableDataType::Variant => Self::Variant,
            }
        }
    }

    impl From<databend_common_expression::types::number::NumberDataType> for NumberDataType {
        fn from(value: databend_common_expression::types::number::NumberDataType) -> Self {
            match value {
                databend_common_expression::types::number::NumberDataType::UInt8 => Self::UInt8,
                databend_common_expression::types::number::NumberDataType::UInt16 => Self::UInt16,
                databend_common_expression::types::number::NumberDataType::UInt32 => Self::UInt32,
                databend_common_expression::types::number::NumberDataType::UInt64 => Self::UInt64,
                databend_common_expression::types::number::NumberDataType::Int8 => Self::Int8,
                databend_common_expression::types::number::NumberDataType::Int16 => Self::Int16,
                databend_common_expression::types::number::NumberDataType::Int32 => Self::Int32,
                databend_common_expression::types::number::NumberDataType::Int64 => Self::Int64,
                databend_common_expression::types::number::NumberDataType::Float32 => Self::Float32,
                databend_common_expression::types::number::NumberDataType::Float64 => Self::Float64,
            }
        }
    }

    impl From<NumberDataType> for databend_common_expression::types::number::NumberDataType {
        fn from(value: NumberDataType) -> Self {
            match value {
                NumberDataType::UInt8 => Self::UInt8,
                NumberDataType::UInt16 => Self::UInt16,
                NumberDataType::UInt32 => Self::UInt32,
                NumberDataType::UInt64 => Self::UInt64,
                NumberDataType::Int8 => Self::Int8,
                NumberDataType::Int16 => Self::Int16,
                NumberDataType::Int32 => Self::Int32,
                NumberDataType::Int64 => Self::Int64,
                NumberDataType::Float32 => Self::Float32,
                NumberDataType::Float64 => Self::Float64,
            }
        }
    }

    impl From<DecimalDataType> for databend_common_expression::types::DecimalDataType {
        fn from(value: DecimalDataType) -> Self {
            match value {
                DecimalDataType::Decimal128(value) => Self::Decimal128(value.into()),
                DecimalDataType::Decimal256(value) => Self::Decimal256(value.into()),
            }
        }
    }

    impl From<DecimalSize> for databend_common_expression::types::DecimalSize {
        fn from(value: DecimalSize) -> Self {
            Self {
                precision: value.precision,
                scale: value.scale,
            }
        }
    }

    impl From<TableSchema> for databend_common_expression::TableSchema {
        fn from(value: TableSchema) -> Self {
            Self {
                fields: value
                    .fields
                    .into_iter()
                    .map(|x| x.into())
                    .collect::<Vec<_>>(),
                metadata: value.metadata,
                next_column_id: value.next_column_id,
            }
        }
    }
}
