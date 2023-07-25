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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::TimeUnit;

use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::with_number_type;
use crate::TableDataType;
use crate::TableField;
use crate::ARROW_EXT_TYPE_BITMAP;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;
use crate::EXTENSION_KEY;

impl From<&TableField> for ArrowField {
    fn from(f: &TableField) -> Self {
        let ty = f.data_type().into();
        let mut metadata = HashMap::new();
        match f.data_type() {
            TableDataType::EmptyArray => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_EMPTY_ARRAY.to_string(),
                );
            }
            TableDataType::EmptyMap => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_EMPTY_MAP.to_string(),
                );
            }
            TableDataType::Variant => {
                metadata.insert(
                    EXTENSION_KEY.to_string(),
                    ARROW_EXT_TYPE_VARIANT.to_string(),
                );
            }
            TableDataType::Bitmap => {
                metadata.insert(EXTENSION_KEY.to_string(), ARROW_EXT_TYPE_BITMAP.to_string());
            }
            _ => Default::default(),
        };
        match ty {
            ArrowDataType::Struct(_) if f.is_nullable() => {
                let ty = set_nullable(&ty);
                ArrowField::new(f.name(), ty, f.is_nullable())
            }
            _ => ArrowField::new(f.name(), ty, f.is_nullable()),
        }
    }
}

impl From<&TableDataType> for ArrowDataType {
    fn from(ty: &TableDataType) -> Self {
        match ty {
            TableDataType::Null => ArrowDataType::Null,
            TableDataType::EmptyArray => ArrowDataType::Null,
            TableDataType::EmptyMap => ArrowDataType::Null,
            TableDataType::Boolean => ArrowDataType::Boolean,
            TableDataType::String => ArrowDataType::LargeBinary,
            TableDataType::Number(ty) => with_number_type!(|TYPE| match ty {
                NumberDataType::TYPE => ArrowDataType::TYPE,
            }),
            TableDataType::Decimal(DecimalDataType::Decimal128(size)) => {
                ArrowDataType::Decimal128(size.precision, size.scale as i8)
            }
            TableDataType::Decimal(DecimalDataType::Decimal256(size)) => {
                ArrowDataType::Decimal256(size.precision, size.scale as i8)
            }
            TableDataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            TableDataType::Date => ArrowDataType::Date32,
            TableDataType::Nullable(ty) => ty.as_ref().into(),
            TableDataType::Array(ty) => {
                let arrow_ty = ty.as_ref().into();
                ArrowDataType::LargeList(FieldRef::from(Arc::new(ArrowField::new(
                    "_array",
                    arrow_ty,
                    ty.is_nullable(),
                ))))
            }
            TableDataType::Map(ty) => {
                let inner_ty = match ty.as_ref() {
                    TableDataType::Tuple {
                        fields_name: _fields_name,
                        fields_type,
                    } => {
                        let key_ty = ArrowDataType::from(&fields_type[0]);
                        let val_ty = ArrowDataType::from(&fields_type[1]);
                        let key_field =
                            ArrowField::new("key", key_ty, fields_type[0].is_nullable());
                        let val_field =
                            ArrowField::new("value", val_ty, fields_type[1].is_nullable());
                        ArrowDataType::Struct(Fields::from(vec![key_field, val_field]))
                    }
                    _ => unreachable!(),
                };
                ArrowDataType::Map(
                    FieldRef::new(ArrowField::new("entries", inner_ty, ty.is_nullable())),
                    false,
                )
            }
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let fields = fields_name
                    .iter()
                    .zip(fields_type)
                    .map(|(name, ty)| ArrowField::new(name.as_str(), ty.into(), ty.is_nullable()))
                    .collect();
                ArrowDataType::Struct(fields)
            }

            TableDataType::Bitmap => ArrowDataType::LargeBinary,
            TableDataType::Variant => ArrowDataType::LargeBinary,
        }
    }
}

pub(super) fn set_nullable(ty: &ArrowDataType) -> ArrowDataType {
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
