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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::binder::JoinPredicate;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::SExpr;
use crate::plans::JoinType;
use crate::plans::ComparisonOp;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AsofJoin {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub non_equi_conditions: Vec<RemoteExpr>,
    // Now only support inner join, will support left/right join later
    pub join_type: JoinType,

    pub output_schema: DataSchemaRef,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AsofJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_asof_join(
        &mut self,
        join: &Join,
        s_expr: &SExpr,
        required: (ColumnSet, ColumnSet),
        mut range_conditions: Vec<ScalarExpr>,
        mut other_conditions: Vec<ScalarExpr>,
    ) -> Result<PhysicalPlan> {

        let mut left = Box::new(self.build(s_expr.child(0)?, required.0).await?);
        let mut right = Box::new(self.build(s_expr.child(1)?, required.1).await?);

        debug_assert!(!range_conditions.is_empty());

        let mut asof_idx = range_conditions.len();

        for (idx, condition) in range_conditions.enumerate() {
             if let ScalarExpr::FunctionCall(func) = condition {
                if func.arguments.len() == 2
                    && matches!(func.func_name.as_str(), "gt" | "lt" | "gte" | "lte")
                {
                    asof_idx = idx;
                }
             }
        }

        debug_assert!(asof_idx < range_conditions.len());

        self.build_range_join(s_expr, left_required, right_required, range_conditions, other_conditions)
                    .await
    }
}