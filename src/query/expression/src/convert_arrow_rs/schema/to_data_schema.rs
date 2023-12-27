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
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;

use crate::types::DataType;
use crate::DataField;
use crate::DataSchema;
use crate::TableDataType;

impl TryFrom<&ArrowSchema> for DataSchema {
    type Error = ArrowError;

    fn try_from(schema: &ArrowSchema) -> Result<Self, ArrowError> {
        let mut fields = vec![];
        for field in &schema.fields {
            fields.push(DataField::try_from(field.as_ref())?)
        }
        Ok(DataSchema {
            fields,
            metadata: Default::default(),
        })
    }
}

impl TryFrom<&ArrowField> for DataField {
    type Error = ArrowError;

    fn try_from(f: &ArrowField) -> Result<Self, ArrowError> {
        let ty = DataType::from(&TableDataType::try_from(f)?);
        Ok(DataField::new(f.name(), ty))
    }
}
