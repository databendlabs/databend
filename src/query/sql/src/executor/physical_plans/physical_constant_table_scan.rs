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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataSchemaRef;

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ConstantTableScan {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub values: Vec<Column>,
    pub num_rows: usize,
    pub output_schema: DataSchemaRef,
}

impl ConstantTableScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }

    pub fn name(&self) -> &str {
        if self.num_rows == 0 {
            "EmptyResultScan"
        } else {
            "ConstantTableScan"
        }
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_constant_table_scan(
        &mut self,
        scan: &crate::plans::ConstantTableScan,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        debug_assert!(scan
            .schema
            .fields
            .iter()
            .map(|field| field.name().parse::<IndexType>().unwrap())
            .collect::<ColumnSet>()
            .is_superset(&scan.columns));

        let used: ColumnSet = required.intersection(&scan.columns).copied().collect();
        if used.len() < scan.columns.len() {
            let crate::plans::ConstantTableScan {
                values,
                num_rows,
                schema,
                ..
            } = scan.prune_columns(used);
            return Ok(PhysicalPlan::ConstantTableScan(ConstantTableScan {
                plan_id: 0,
                values,
                num_rows,
                output_schema: schema,
            }));
        }

        Ok(PhysicalPlan::ConstantTableScan(ConstantTableScan {
            plan_id: 0,
            values: scan.values.clone(),
            num_rows: scan.num_rows,
            output_schema: scan.schema.clone(),
        }))
    }
}
