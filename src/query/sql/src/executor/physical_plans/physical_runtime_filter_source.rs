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

use std::collections::BTreeMap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check;
use common_expression::type_check::common_super_type;
use common_expression::ConstantFolder;
use common_expression::DataSchemaRef;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::plans::RuntimeFilterId;
use crate::ColumnSet;
use crate::TypeCheck;

// Build runtime predicate data from join build side
// Then pass it to runtime filter on join probe side
// It's the children of join node
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeFilterSource {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left_side: Box<PhysicalPlan>,
    pub right_side: Box<PhysicalPlan>,
    pub left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
}

impl RuntimeFilterSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.left_side.output_schema()
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_runtime_filter(
        &mut self,
        s_expr: &SExpr,
        runtime_filter: &crate::plans::RuntimeFilterSource,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let left_required = runtime_filter
            .left_runtime_filters
            .iter()
            .fold(required.clone(), |acc, v| {
                acc.union(&v.1.used_columns()).cloned().collect()
            });
        let right_required = runtime_filter
            .right_runtime_filters
            .iter()
            .fold(required, |acc, v| {
                acc.union(&v.1.used_columns()).cloned().collect()
            });

        // 2. Build physical plan.
        let left_side = Box::new(self.build(s_expr.child(0)?, left_required).await?);
        let left_schema = left_side.output_schema()?;
        let right_side = Box::new(self.build(s_expr.child(1)?, right_required).await?);
        let right_schema = right_side.output_schema()?;
        let mut left_runtime_filters = BTreeMap::new();
        let mut right_runtime_filters = BTreeMap::new();
        for (left, right) in runtime_filter
            .left_runtime_filters
            .iter()
            .zip(runtime_filter.right_runtime_filters.iter())
        {
            let left_expr = left
                .1
                .resolve_and_check(left_schema.as_ref())?
                .project_column_ref(|index| left_schema.index_of(&index.to_string()).unwrap());
            let right_expr = right
                .1
                .resolve_and_check(right_schema.as_ref())?
                .project_column_ref(|index| right_schema.index_of(&index.to_string()).unwrap());

            let common_ty = common_super_type(left_expr.data_type().clone(), right_expr.data_type().clone(), &BUILTIN_FUNCTIONS.default_cast_rules)
                .ok_or_else(|| ErrorCode::SemanticError(format!("RuntimeFilter's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}", left.0, left_expr.data_type(), right.0, right_expr.data_type())))?;

            let left_expr = type_check::check_cast(
                left_expr.span(),
                false,
                left_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;
            let right_expr = type_check::check_cast(
                right_expr.span(),
                false,
                right_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;

            let (left_expr, _) =
                ConstantFolder::fold(&left_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let (right_expr, _) =
                ConstantFolder::fold(&right_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

            left_runtime_filters.insert(left.0.clone(), left_expr.as_remote_expr());
            right_runtime_filters.insert(right.0.clone(), right_expr.as_remote_expr());
        }
        Ok(PhysicalPlan::RuntimeFilterSource(RuntimeFilterSource {
            plan_id: self.next_plan_id(),
            left_side,
            right_side,
            left_runtime_filters,
            right_runtime_filters,
        }))
    }
}
