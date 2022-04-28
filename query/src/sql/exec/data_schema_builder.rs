// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::sql::exec::util::format_field_name;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::ProjectPlan;
use crate::sql::IndexType;
use crate::sql::Metadata;

pub struct DataSchemaBuilder<'a> {
    metadata: &'a Metadata,
}

impl<'a> DataSchemaBuilder<'a> {
    pub fn new(metadata: &'a Metadata) -> Self {
        DataSchemaBuilder { metadata }
    }

    pub fn build_project(
        &self,
        plan: &ProjectPlan,
        input_schema: DataSchemaRef,
    ) -> Result<DataSchemaRef> {
        let mut fields = input_schema.fields().clone();
        for item in plan.items.iter() {
            let index = item.index;
            let column_entry = self.metadata.column(index);
            let field_name = format_field_name(column_entry.name.as_str(), index);
            let field = if column_entry.nullable {
                DataField::new_nullable(field_name.as_str(), column_entry.data_type.clone())
            } else {
                DataField::new(field_name.as_str(), column_entry.data_type.clone())
            };
            fields.push(field);
        }

        Ok(Arc::new(DataSchema::new(fields)))
    }

    pub fn build_physical_scan(&self, plan: &PhysicalScan) -> Result<DataSchemaRef> {
        let mut fields: Vec<DataField> = vec![];
        for index in plan.columns.iter() {
            let column_entry = self.metadata.column(*index);
            let field_name = format_field_name(column_entry.name.as_str(), *index);
            let field = if column_entry.nullable {
                DataField::new_nullable(field_name.as_str(), column_entry.data_type.clone())
            } else {
                DataField::new(field_name.as_str(), column_entry.data_type.clone())
            };

            fields.push(field);
        }

        Ok(Arc::new(DataSchema::new(fields)))
    }

    pub fn build_canonical_schema(&self, columns: &[IndexType]) -> DataSchemaRef {
        let mut fields: Vec<DataField> = vec![];
        for index in columns {
            let column_entry = self.metadata.column(*index);
            let field_name = column_entry.name.clone();
            let field = if column_entry.nullable {
                DataField::new_nullable(field_name.as_str(), column_entry.data_type.clone())
            } else {
                DataField::new(field_name.as_str(), column_entry.data_type.clone())
            };

            fields.push(field);
        }

        Arc::new(DataSchema::new(fields))
    }
}
