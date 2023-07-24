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

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Fields;
use arrow_schema::Schema as ArrowSchema;

use super::set_nullable;
use crate::infer_schema_type;
use crate::types::DataType;
use crate::DataField;
use crate::DataSchema;

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
        match ty {
            ArrowDataType::Struct(_) if f.is_nullable() => {
                let ty = set_nullable(&ty);
                ArrowField::new(f.name(), ty, f.is_nullable())
            }
            _ => ArrowField::new(f.name(), ty, f.is_nullable()),
        }
    }
}

impl From<&DataType> for ArrowDataType {
    fn from(ty: &DataType) -> Self {
        (&infer_schema_type(ty).expect("Generic type can not convert to arrow")).into()
    }
}
