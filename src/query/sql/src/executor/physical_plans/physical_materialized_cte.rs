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

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::ColumnBinding;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializedCTE {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub input: Box<PhysicalPlan>,
    pub cte_name: String,
    pub cte_output_columns: Option<Vec<ColumnBinding>>,
    pub ref_count: usize,
    pub channel_size: Option<usize>,
}

impl MaterializedCTE {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_materialized_cte(
        &mut self,
        s_expr: &SExpr,
        materialized_cte: &crate::plans::MaterializedCTE,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let required = match &materialized_cte.cte_output_columns {
            Some(o) => o.iter().map(|c| c.index).collect(),
            None => RelExpr::with_s_expr(s_expr.child(0)?)
                .derive_relational_prop()?
                .output_columns
                .clone(),
        };
        let input = Box::new(self.build(s_expr.child(0)?, required).await?);
        Ok(PhysicalPlan::MaterializedCTE(Box::new(MaterializedCTE {
            plan_id: 0,
            stat_info: Some(stat_info),
            input,
            cte_name: materialized_cte.cte_name.clone(),
            cte_output_columns: materialized_cte.cte_output_columns.clone(),
            ref_count: materialized_cte.ref_count,
            channel_size: materialized_cte.channel_size,
        })))
    }
}
