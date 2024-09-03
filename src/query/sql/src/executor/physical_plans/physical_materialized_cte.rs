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

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::ColumnBinding;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializedCte {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub cte_idx: IndexType,
    pub materialized_output_columns: Vec<ColumnBinding>,
}

impl MaterializedCte {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let fields = self.left.output_schema()?.fields().clone();
        Ok(DataSchemaRefExt::create(fields))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_materialized_cte(
        &mut self,
        s_expr: &SExpr,
        cte: &crate::plans::MaterializedCte,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let left_output_column = RelExpr::with_s_expr(s_expr)
            .derive_relational_prop_child(0)?
            .output_columns
            .clone();
        let right_used_column = RelExpr::with_s_expr(s_expr)
            .derive_relational_prop_child(1)?
            .used_columns
            .clone();
        // Get the intersection of `left_used_column` and `right_used_column`
        let materialize_required = left_output_column
            .iter()
            .map(|index| {
                if let Some(materialized_index) = cte.materialized_indexes.get(index) {
                    *materialized_index
                } else {
                    *index
                }
            })
            .collect::<ColumnSet>()
            .intersection(&right_used_column)
            .cloned()
            .collect::<ColumnSet>();

        let mut materialized_output_columns = vec![];
        for column in cte.materialized_output_columns.iter() {
            if materialize_required.contains(&column.index) {
                materialized_output_columns.push(column.clone());
            }
        }
        self.cte_output_columns
            .insert(cte.cte_idx, materialized_output_columns.clone());

        let materialized_right =
            Box::new(self.build(s_expr.child(1)?, materialize_required).await?);
        let left = Box::new(self.build(s_expr.child(0)?, required).await?);

        // 2. Build physical plan.
        Ok(PhysicalPlan::MaterializedCte(MaterializedCte {
            plan_id: 0,
            left,
            right: materialized_right,
            cte_idx: cte.cte_idx,
            materialized_output_columns,
        }))
    }
}
