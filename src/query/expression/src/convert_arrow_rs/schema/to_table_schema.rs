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

use arrow_schema::ArrowError;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;

use crate::types::DecimalDataType;
use crate::types::DecimalSize;
use crate::types::NumberDataType;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;
use crate::ARROW_EXT_TYPE_BITMAP;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;
use crate::EXTENSION_KEY;

impl TryFrom<&ArrowSchema> for TableSchema {
    type Error = ArrowError;

    fn try_from(a_schema: &ArrowSchema) -> Result<Self, ArrowError> {
        let fields: Result<Vec<TableField>, ArrowError> = a_schema
            .fields
            .iter()
            .map(|arrow_f| arrow_f.as_ref().try_into())
            .collect();

        Ok(TableSchema::new(fields?))
    }
}

impl TryFrom<&ArrowField> for TableField {
    type Error = ArrowError;

    fn try_from(f: &ArrowField) -> Result<Self, ArrowError> {
        Ok(Self {
            name: f.name().clone(),
            data_type: f.try_into()?,
            default_expr: None,
            column_id: 0,
            computed_expr: None,
        })
    }
}

impl TryFrom<&ArrowField> for TableDataType {
    type Error = ArrowError;

    fn try_from(f: &ArrowField) -> Result<Self, ArrowError> {
        let extend_type = match f.metadata().get(EXTENSION_KEY).map(|v| v.as_str()) {
            Some(ARROW_EXT_TYPE_EMPTY_ARRAY) => Some(TableDataType::EmptyArray),
            Some(ARROW_EXT_TYPE_EMPTY_MAP) => Some(TableDataType::EmptyMap),
            Some(ARROW_EXT_TYPE_VARIANT) => Some(TableDataType::Variant),
            Some(ARROW_EXT_TYPE_BITMAP) => Some(TableDataType::Bitmap),
            _ => None,
        };

        if let Some(ty) = extend_type {
            return if f.is_nullable() {
                Ok(ty.wrap_nullable())
            } else {
                Ok(ty)
            };
        }

        let ty = f.data_type();
        let data_type = match ty {
            ArrowDataType::Null => TableDataType::Null,
            ArrowDataType::Boolean => TableDataType::Boolean,
            ArrowDataType::Int8 => TableDataType::Number(NumberDataType::Int8),
            ArrowDataType::Int16 => TableDataType::Number(NumberDataType::Int16),
            ArrowDataType::Int32 => TableDataType::Number(NumberDataType::Int32),
            ArrowDataType::Int64 => TableDataType::Number(NumberDataType::Int64),
            ArrowDataType::UInt8 => TableDataType::Number(NumberDataType::UInt8),
            ArrowDataType::UInt16 => TableDataType::Number(NumberDataType::UInt16),
            ArrowDataType::UInt32 => TableDataType::Number(NumberDataType::UInt32),
            ArrowDataType::UInt64 => TableDataType::Number(NumberDataType::UInt64),
            ArrowDataType::Float32 | ArrowDataType::Float16 => {
                TableDataType::Number(NumberDataType::Float32)
            }
            ArrowDataType::Float64 => TableDataType::Number(NumberDataType::Float64),
            ArrowDataType::Timestamp(_unit, _tz) => TableDataType::Timestamp,
            ArrowDataType::Date32 | ArrowDataType::Date64 => TableDataType::Date,
            ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Binary
            | ArrowDataType::LargeBinary => TableDataType::String,
            ArrowDataType::Decimal128(p, s) => {
                TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: *p,
                    scale: (*s) as u8,
                }))
            }
            ArrowDataType::Decimal256(p, s) => {
                TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                    precision: *p,
                    scale: (*s) as u8,
                }))
            }
            ArrowDataType::List(f) => TableDataType::Array(Box::new(f.as_ref().try_into()?)),
            ArrowDataType::LargeList(f) => TableDataType::Array(Box::new(f.as_ref().try_into()?)),
            ArrowDataType::FixedSizeList(f, _) => {
                TableDataType::Array(Box::new((&*(*f)).try_into()?))
            }
            ArrowDataType::Map(f, _) => {
                let inner_ty = f.as_ref().try_into()?;
                TableDataType::Map(Box::new(inner_ty))
            }
            ArrowDataType::Struct(fields) => {
                let fields_name = fields.iter().map(|f| f.name().clone()).collect::<Vec<_>>();
                let fields_type: Result<Vec<TableDataType>, ArrowError> = fields
                    .iter()
                    .map(|f| TableDataType::try_from(f.as_ref()))
                    .collect();
                let fields_type = fields_type?;
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                }
            }
            _ => Err(ArrowError::CastError(format!(
                "cast {ty} to TableDataType not implemented yet"
            )))?,
        };
        if f.is_nullable() {
            Ok(data_type.wrap_nullable())
        } else {
            Ok(data_type)
        }
    }
}
