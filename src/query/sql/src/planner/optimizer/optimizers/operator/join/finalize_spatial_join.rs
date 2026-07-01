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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::RelOperator;
use crate::plans::spatial_join_gate;

/// Finalize derived spatial join annotations after logical rewrites have settled.
pub struct FinalizeSpatialJoinOptimizer {
    ctx: Arc<OptimizerContext>,
}

impl FinalizeSpatialJoinOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        Self { ctx }
    }

    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        if !self
            .ctx
            .get_table_ctx()
            .get_settings()
            .get_enable_spatial_join()?
        {
            return Ok(s_expr.clone());
        }

        Self::finalize_spatial_join(s_expr)
    }

    #[recursive::recursive]
    fn finalize_spatial_join(s_expr: &SExpr) -> Result<SExpr> {
        let mut changed = false;
        let mut children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            let new_child = Self::finalize_spatial_join(child)?;
            changed |= !new_child.eq(child);
            children.push(Arc::new(new_child));
        }

        let mut result = if changed {
            s_expr.replace_children(children)
        } else {
            s_expr.clone()
        };

        if let RelOperator::Join(join) = result.plan() {
            let left_prop = RelExpr::with_s_expr(result.left_child()).derive_relational_prop()?;
            let right_prop = RelExpr::with_s_expr(result.right_child()).derive_relational_prop()?;
            let spatial_join =
                spatial_join_gate(join, &left_prop.output_columns, &right_prop.output_columns)
                    .map(Box::new);

            if join.spatial_join != spatial_join {
                let mut join = join.clone();
                join.spatial_join = spatial_join;
                result = result.replace_plan(Arc::new(RelOperator::Join(join)));
            }
        }

        Ok(result)
    }
}

#[async_trait::async_trait]
impl Optimizer for FinalizeSpatialJoinOptimizer {
    fn name(&self) -> String {
        "FinalizeSpatialJoinOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
