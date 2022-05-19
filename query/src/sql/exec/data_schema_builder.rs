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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::Expression;

use crate::sql::exec::util::format_field_name;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::Project;
use crate::sql::IndexType;
use crate::sql::Metadata;

pub struct DataSchemaBuilder<'a> {
    metadata: &'a Metadata,
}

impl<'a> DataSchemaBuilder<'a> {
    pub fn new(metadata: &'a Metadata) -> Self {
        DataSchemaBuilder { metadata }
    }

    pub fn build_project(&self, plan: &Project) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(plan.columns.len());
        for index in plan.columns.iter() {
            let column_entry = self.metadata.column(*index);
            let field_name = format_field_name(column_entry.name.as_str(), *index);
            let field = DataField::new(field_name.as_str(), column_entry.data_type.clone());
            fields.push(field);
        }

        Ok(DataSchemaRefExt::create(fields))
    }

    pub fn build_eval_scalar(
        &self,
        plan: &EvalScalar,
        input_schema: DataSchemaRef,
    ) -> Result<DataSchemaRef> {
        let mut fields = input_schema.fields().clone();
        for item in plan.items.iter() {
            let column_entry = self.metadata.column(item.index);
            let field_name = format_field_name(column_entry.name.as_str(), item.index);
            let field = DataField::new(field_name.as_str(), column_entry.data_type.clone());
            fields.push(field);
        }

        Ok(DataSchemaRefExt::create(fields))
    }

    pub fn build_physical_scan(&self, plan: &PhysicalScan) -> Result<DataSchemaRef> {
        let mut fields: Vec<DataField> = vec![];
        for index in plan.columns.iter() {
            let column_entry = self.metadata.column(*index);
            let field_name = format_field_name(column_entry.name.as_str(), *index);
            let field = DataField::new(field_name.as_str(), column_entry.data_type.clone());
            fields.push(field);
        }

        Ok(DataSchemaRefExt::create(fields))
    }

    pub fn build_canonical_schema(&self, columns: &[IndexType]) -> DataSchemaRef {
        let mut fields: Vec<DataField> = vec![];
        for index in columns {
            let column_entry = self.metadata.column(*index);
            let field_name = column_entry.name.clone();
            let field = DataField::new(field_name.as_str(), column_entry.data_type.clone());
            fields.push(field);
        }

        DataSchemaRefExt::create(fields)
    }

    pub fn build_join(&self, left: DataSchemaRef, right: DataSchemaRef) -> DataSchemaRef {
        // TODO: NATURAL JOIN and USING
        let mut fields = Vec::with_capacity(left.num_fields() + right.num_fields());
        for field in left.fields().iter() {
            fields.push(field.clone());
        }
        for field in right.fields().iter() {
            fields.push(field.clone());
        }

        DataSchemaRefExt::create(fields)
    }

    pub fn build_sort(
        &self,
        input_schema: &DataSchemaRef,
        exprs: &[Expression],
    ) -> Result<DataSchemaRef> {
        let mut fields = input_schema.fields().clone();
        for expr in exprs.iter() {
            let expr_name = expr.column_name().clone();
            if !input_schema.has_field(expr_name.as_str()) {
                let field = DataField::new(expr_name.as_str(), expr.to_data_type(input_schema)?);
                fields.push(field);
            }
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}
