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
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::SExpr;
use crate::sql::Metadata;
use crate::sql::PhysicalProject;
use crate::sql::PhysicalScan;
use crate::sql::Plan;

pub struct DataSchemaHelper;

impl DataSchemaHelper {
    pub fn build(expr: &SExpr, metadata: &Metadata) -> Result<DataSchema> {
        match expr.plan().as_ref() {
            Plan::PhysicalScan(plan) => Self::build_scan(plan, metadata),
            Plan::PhysicalProject(plan) => Self::build_project(plan, &expr.children()[0], metadata),
            _ => Err(ErrorCode::LogicalError(format!(
                "Invalid physical plan: {:?}",
                expr.plan()
            ))),
        }
    }

    pub fn build_scan(scan: &PhysicalScan, metadata: &Metadata) -> Result<DataSchema> {
        let column_entries = metadata.columns_by_table_index(scan.table_index);
        let mut data_fields = vec![];
        for column_entry in column_entries.iter() {
            let field = DataField::new(column_entry.name.as_str(), column_entry.data_type.clone());
            data_fields.push(field)
        }
        let result = DataSchema::new(data_fields);

        Ok(result)
    }

    pub fn build_project(
        project: &PhysicalProject,
        child: &SExpr,
        metadata: &Metadata,
    ) -> Result<DataSchema> {
        let _input_schema = Self::build(child, metadata)?;
        let mut data_fields = vec![];
        for item in project.items.iter() {
            let column_entry = metadata.column(item.index);
            let field = DataField::new(column_entry.name.as_str(), column_entry.data_type.clone());
            data_fields.push(field);
        }
        let result = DataSchema::new(data_fields);

        Ok(result)
    }
}
