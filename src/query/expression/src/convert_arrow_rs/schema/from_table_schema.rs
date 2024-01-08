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
use arrow_schema::Schema;
use arrow_schema::TimeUnit;

use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::with_number_type;
use crate::TableDataType;
use crate::TableField;
use crate::TableSchema;
use crate::ARROW_EXT_TYPE_BITMAP;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;
use crate::EXTENSION_KEY;

impl From<&TableField> for ArrowField {
    fn from(f: &TableField) -> Self {
        let ty = f.data_type().into();
        let extend_type = match f.data_type().remove_nullable() {
            TableDataType::EmptyArray => Some(ARROW_EXT_TYPE_EMPTY_ARRAY.to_string()),
            TableDataType::EmptyMap => Some(ARROW_EXT_TYPE_EMPTY_MAP.to_string()),
            TableDataType::Variant => Some(ARROW_EXT_TYPE_VARIANT.to_string()),
            TableDataType::Bitmap => Some(ARROW_EXT_TYPE_BITMAP.to_string()),
            _ => None,
        };

        if let Some(extend_type) = extend_type {
            let mut metadata = HashMap::new();
            metadata.insert(EXTENSION_KEY.to_string(), extend_type);
            ArrowField::new(f.name(), ty, f.is_nullable_or_null()).with_metadata(metadata)
        } else {
            ArrowField::new(f.name(), ty, f.is_nullable_or_null())
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

            TableDataType::Binary => ArrowDataType::LargeBinary,
            TableDataType::Bitmap => ArrowDataType::LargeBinary,
            TableDataType::Variant => ArrowDataType::LargeBinary,
        }
    }
}

impl From<&TableSchema> for Schema {
    fn from(value: &TableSchema) -> Self {
        let fields = Fields::from_iter(value.fields.iter().map(|f| Arc::new(f.into())));
        let metadata = value
            .metadata
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        Schema::new_with_metadata(fields, metadata)
    }
}
