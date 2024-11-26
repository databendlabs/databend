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

use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::MutationKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;

impl PhysicalPlanBuilder {
    pub async fn build_optimize_cluster_by(
        &mut self,
        s_expr: &SExpr,
        _optimize: &crate::plans::OptimizeClusterBy,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let mut plan = self.build(s_expr.child(0)?, required).await?;
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
