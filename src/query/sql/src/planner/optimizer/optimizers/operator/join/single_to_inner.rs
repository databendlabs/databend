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
use crate::optimizer::ir::SExpr;
use crate::plans::JoinType;
use crate::plans::RelOperator;

// The SingleToInnerOptimizer will convert some single join to inner join.
pub struct SingleToInnerOptimizer {}

impl SingleToInnerOptimizer {
    pub fn new() -> Self {
        SingleToInnerOptimizer {}
    }

    pub fn optimize_sync(&mut self, s_expr: SExpr) -> Result<SExpr> {
        Self::single_to_inner(s_expr)
    }

    #[recursive::recursive]
    fn single_to_inner(mut s_expr: SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(s_expr.children.len());
        for child in std::mem::take(&mut s_expr.children) {
            children.push(Arc::new(Self::single_to_inner(Arc::unwrap_or_clone(
                child,
            ))?));
        }
        let mut s_expr = s_expr.replace_children(children);

        if let RelOperator::Join(join) = Arc::make_mut(&mut s_expr.plan)
            && join.single_to_inner.is_some()
        {
            join.join_type = JoinType::Inner;
        }

        Ok(s_expr)
    }
}

impl Default for SingleToInnerOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Optimizer for SingleToInnerOptimizer {
    fn name(&self) -> String {
        "SingleToInnerOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
