// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::Expression;
use crate::PlanBuilder;
use crate::PlanNode;
use crate::ScanPlan;

pub struct TableScanInfo<'a> {
    pub table_name: &'a str,
    pub table_id: u64,
    pub table_version: Option<u64>,
    pub table_schema: &'a DataSchema,
    pub table_args: Option<Expression>,
}

impl PlanBuilder {
    /// Scan a data source
    pub fn scan(schema_name: &str, table_scan_info: TableScanInfo) -> Result<PlanBuilder> {
        let table_schema = DataSchemaRef::new(table_scan_info.table_schema.clone());

        Ok(PlanBuilder::from(&PlanNode::Scan(ScanPlan {
            schema_name: schema_name.to_owned(),
            table_id: table_scan_info.table_id,
            table_version: table_scan_info.table_version,
            table_schema,
            table_args: table_scan_info.table_args,
        })))
    }
}
