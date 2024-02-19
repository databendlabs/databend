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
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CteScan {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub cte_idx: (IndexType, IndexType),
    pub output_schema: DataSchemaRef,
    pub offsets: Vec<IndexType>,
    pub stat: PlanStatsInfo,
}

impl CteScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_cte_scan(
        &mut self,
        cte_scan: &crate::plans::CteScan,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut used_columns = cte_scan.used_columns()?;
        used_columns = required.intersection(&used_columns).cloned().collect();
        let mut pruned_fields = vec![];
        let mut pruned_offsets = vec![];
        let cte_output_columns = self.cte_output_columns.get(&cte_scan.cte_idx.0).unwrap();
        for field in cte_scan.fields.iter() {
            if used_columns.contains(&field.name().parse()?) {
                pruned_fields.push(field.clone());
            }
        }
        for field in pruned_fields.iter() {
            for (offset, col) in cte_output_columns.iter().enumerate() {
                if col.index.eq(&field.name().parse::<IndexType>()?) {
                    pruned_offsets.push(offset);
                    break;
                }
            }
        }

        let plan_stat = PlanStatsInfo {
            estimated_rows: cte_scan.stat.cardinality,
        };

        // 2. Build physical plan.
        Ok(PhysicalPlan::CteScan(CteScan {
            plan_id: 0,
            cte_idx: cte_scan.cte_idx,
            output_schema: DataSchemaRefExt::create(pruned_fields),
            offsets: pruned_offsets,
            stat: plan_stat,
        }))
    }
}
