// Copyright 2023 Datafuse Labs.
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
use arrow_schema::TimeUnit;

use crate::types::decimal::DecimalSize;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::with_number_type;
use crate::DataField;
use crate::DataSchema;

impl From<&DataType> for ArrowDataType {
    fn from(ty: &DataType) -> Self {
        match ty {
            DataType::Null => ArrowDataType::Null,

            DataType::Boolean => ArrowDataType::Boolean,
            DataType::String => ArrowDataType::LargeUtf8,
            DataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            DataType::Decimal(DecimalDataType::Decimal128(s)) => {
                ArrowDataType::Decimal128(s.precision, s.scale as i8)
            }
            DataType::Decimal(DecimalDataType::Decimal256(s)) => {
                ArrowDataType::Decimal256(s.precision, s.scale as i8)
            }
            DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Date => ArrowDataType::Date32,
            DataType::Nullable(ty) => ty.as_ref().into(),
            DataType::Array(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(Box::new(ArrowField::new(
                    "_array",
                    arrow_ty,
                    ty.is_nullable(),
                )))
            }
            DataType::Map(ty) => {
                let inner_ty = match ty.as_ref() {
                    DataType::Tuple(tys) => {
                        let key_ty = ArrowDataType::from(&tys[0]);
                        let val_ty = ArrowDataType::from(&tys[1]);
                        let key_field = ArrowField::new("key", key_ty, tys[0].is_nullable());
                        let val_field = ArrowField::new("value", val_ty, tys[1].is_nullable());
                        ArrowDataType::Struct(vec![key_field, val_field])
                    }
                    _ => unreachable!(),
                };
                ArrowDataType::Map(
                    Box::new(ArrowField::new("entries", inner_ty, ty.is_nullable())),
                    false,
                )
            }
            DataType::Tuple(types) => {
                let fields = types
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| {
                        let index = index + 1;
                        let name = format!("{index}");
                        ArrowField::new(name.as_str(), ty.into(), ty.is_nullable())
                    })
                    .collect();
                ArrowDataType::Struct(fields)
            }

            DataType::EmptyArray => ArrowDataType::Null,
            DataType::EmptyMap => ArrowDataType::Null,
            DataType::Variant => ArrowDataType::LargeBinary,

            _ => unreachable!(),
        }
    }
}

fn set_nullable(ty: &ArrowDataType) -> ArrowDataType {
    // if the struct type is nullable, need to set inner fields as nullable
    match ty {
        ArrowDataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|f| {
                    let data_type = set_nullable(f.data_type());
                    ArrowField::new(f.name().clone(), data_type, true)
                })
                .collect();
            ArrowDataType::Struct(fields)
        }
        _ => ty.clone(),
    }
}

impl From<&DataField> for ArrowField {
    fn from(f: &DataField) -> Self {
        let ty = f.data_type().into();
        match ty {
            ArrowDataType::Struct(_) if f.is_nullable() => {
                let ty = set_nullable(&ty);
                ArrowField::new(f.name(), ty, f.is_nullable())
            }
            _ => ArrowField::new(f.name(), ty, f.is_nullable()),
        }
    }
}

impl From<&DataSchema> for ArrowSchema {
    fn from(value: &DataSchema) -> Self {
        ArrowSchema {
            fields: value.fields.iter().map(|f| f.into()).collect::<Vec<_>>(),
            metadata: Default::default(),
        }
    }
}

impl TryFrom<&ArrowField> for DataField {
    type Error = ArrowError;

    fn try_from(f: &ArrowField) -> Result<Self, ArrowError> {
        let ty = f.data_type().try_into()?;
        if f.is_nullable() {
            Ok(DataField::new_nullable(f.name().as_str(), ty))
        } else {
            Ok(DataField::new(f.name(), ty))
        }
    }
}

impl TryFrom<&ArrowSchema> for DataSchema {
    type Error = ArrowError;

    fn try_from(schema: &ArrowSchema) -> Result<Self, ArrowError> {
        let mut fields = vec![];
        for field in &schema.fields {
            fields.push(DataField::try_from(field)?)
        }
        Ok(DataSchema {
            fields,
            metadata: Default::default(),
        })
    }
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = ArrowError;

    fn try_from(ty: &ArrowDataType) -> Result<Self, ArrowError> {
        let data_type = match ty {
            ArrowDataType::Null => DataType::Null,
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Int8 => DataType::Number(NumberDataType::Int8),
            ArrowDataType::Int16 => DataType::Number(NumberDataType::Int16),
            ArrowDataType::Int32 => DataType::Number(NumberDataType::Int32),
            ArrowDataType::Int64 => DataType::Number(NumberDataType::Int64),
            ArrowDataType::UInt8 => DataType::Number(NumberDataType::UInt8),
            ArrowDataType::UInt16 => DataType::Number(NumberDataType::UInt16),
            ArrowDataType::UInt32 => DataType::Number(NumberDataType::UInt32),
            ArrowDataType::UInt64 => DataType::Number(NumberDataType::UInt64),
            ArrowDataType::Float32 | ArrowDataType::Float16 => {
                DataType::Number(NumberDataType::Float32)
            }
            ArrowDataType::Float64 => DataType::Number(NumberDataType::Float64),
            ArrowDataType::Timestamp(_unit, _tz) => DataType::Timestamp,
            ArrowDataType::Date32 | ArrowDataType::Date64 => DataType::Date,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,
            ArrowDataType::Decimal128(p, s) => {
                DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: *p,
                    scale: (*s) as u8,
                }))
            }
            ArrowDataType::Decimal256(p, s) => {
                DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                    precision: *p,
                    scale: (*s) as u8,
                }))
            }
            _ => Err(ArrowError::CastError(format!(
                "cast {ty} to DataType not not implemented yet"
            )))?,
        };
        Ok(data_type)
    }
}
