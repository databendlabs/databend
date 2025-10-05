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

use async_trait::async_trait;
use databend_common_exception::Result;

use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::common_subexpression::analyze::analyze_common_subexpression;
use crate::optimizer::optimizers::common_subexpression::rewrite::rewrite_sexpr;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;

pub struct CommonSubexpressionOptimizer {
    pub(crate) _opt_ctx: Arc<OptimizerContext>,
}

#[async_trait]
impl Optimizer for CommonSubexpressionOptimizer {
    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let metadata = self._opt_ctx.get_metadata();
        let metadata = metadata.read();
        let (replacements, materialized_ctes) = analyze_common_subexpression(s_expr, &metadata)?;
        rewrite_sexpr(s_expr, replacements, materialized_ctes)
    }

    fn name(&self) -> String {
        "CommonSubexpressionOptimizer".to_string()
    }
}

impl CommonSubexpressionOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self { _opt_ctx: opt_ctx }
    }
}
