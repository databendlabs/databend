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

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Fields;
use arrow_schema::Schema as ArrowSchema;

use super::set_nullable;
use crate::infer_schema_type;
use crate::types::DataType;
use crate::DataField;
use crate::DataSchema;
use crate::ARROW_EXT_TYPE_BITMAP;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;
use crate::EXTENSION_KEY;

impl From<&DataSchema> for ArrowSchema {
    fn from(value: &DataSchema) -> Self {
        let fields: Vec<ArrowField> = value.fields.iter().map(|f| f.into()).collect::<Vec<_>>();
        ArrowSchema {
            fields: Fields::from(fields),
            metadata: Default::default(),
        }
    }
}

impl From<&DataField> for ArrowField {
    fn from(f: &DataField) -> Self {
        let ty = f.data_type().into();
        let extend_type = match f.data_type().remove_nullable() {
            DataType::EmptyArray => Some(ARROW_EXT_TYPE_EMPTY_ARRAY.to_string()),
            DataType::EmptyMap => Some(ARROW_EXT_TYPE_EMPTY_MAP.to_string()),
            DataType::Variant => Some(ARROW_EXT_TYPE_VARIANT.to_string()),
            DataType::Bitmap => Some(ARROW_EXT_TYPE_BITMAP.to_string()),
            _ => None,
        };

        if let Some(extend_type) = extend_type {
            let mut metadata = HashMap::new();
            metadata.insert(EXTENSION_KEY.to_string(), extend_type);
            ArrowField::new(f.name(), ty, f.is_nullable_or_null()).with_metadata(metadata)
        } else {
            match ty {
                ArrowDataType::Struct(_) if f.is_nullable() => {
                    let ty = set_nullable(&ty);
                    ArrowField::new(f.name(), ty, f.is_nullable())
                }
                _ => ArrowField::new(f.name(), ty, f.is_nullable_or_null()),
            }
        }
    }
}

impl From<&DataType> for ArrowDataType {
    fn from(ty: &DataType) -> Self {
        (&infer_schema_type(ty).expect("Generic type can not convert to arrow")).into()
    }
}

/// This function is similar to the above, but does not change the nullability of any type.
pub fn data_schema_to_arrow_schema(data_schema: &DataSchema) -> ArrowSchema {
    let fields = data_schema
        .fields
        .iter()
        .map(|f| {
            let ty = f.data_type().into();
            ArrowField::new(f.name(), ty, f.is_nullable_or_null())
        })
        .collect::<Vec<_>>();
    ArrowSchema {
        fields: Fields::from(fields),
        metadata: Default::default(),
    }
}
