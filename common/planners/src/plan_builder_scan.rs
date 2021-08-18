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

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;

use crate::Expression;
use crate::Extras;
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
    pub fn scan(
        schema_name: &str,
        table_scan_info: TableScanInfo,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<PlanBuilder> {
        let table_schema = DataSchemaRef::new(table_scan_info.table_schema.clone());
        let projected_schema = projection.clone().map(|p| {
            DataSchemaRefExt::create(p.iter().map(|i| table_schema.field(*i).clone()).collect())
                .as_ref()
                .clone()
        });
        let projected_schema = match projected_schema {
            None => table_schema.clone(),
            Some(v) => Arc::new(v),
        };

        Ok(PlanBuilder::from(&PlanNode::Scan(ScanPlan {
            schema_name: schema_name.to_owned(),
            table_id: table_scan_info.table_id,
            table_version: table_scan_info.table_version,
            table_schema,
            projected_schema,
            table_args: table_scan_info.table_args,
            push_downs: Extras {
                projection,
                filters: vec![],
                limit,
            },
        })))
    }
}
