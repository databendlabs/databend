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

use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
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
    pub cte_scan_offset: Vec<usize>,
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
        self.cte_output_columns
            .insert(cte.cte_idx, cte.materialized_output_columns.clone());
        self.cet_used_column_offsets
            .insert(cte.cte_idx, HashSet::new());
        let left = Box::new(self.build(s_expr.child(0)?, required).await?);

        let mut materialize_required = ColumnSet::new();
        let mut materialized_output_columns = vec![];
        let mut cte_scan_offset = Vec::with_capacity(cte.materialized_output_columns.len());
        let used_column_offset = self.cet_used_column_offsets.get(&cte.cte_idx).unwrap();
        for (offset, column) in cte.materialized_output_columns.iter().enumerate() {
            if used_column_offset.contains(&offset) {
                cte_scan_offset.push(materialized_output_columns.len());
                materialize_required.insert(column.index);
                materialized_output_columns.push(column.clone());
            } else {
                cte_scan_offset.push(0);
            }
        }
        let right = Box::new(self.build(s_expr.child(1)?, materialize_required).await?);

        // 2. Build physical plan.
        Ok(PhysicalPlan::MaterializedCte(MaterializedCte {
            plan_id: 0,
            left,
            right,
            cte_idx: cte.cte_idx,
            cte_scan_offset,
            materialized_output_columns,
        }))
    }
}
