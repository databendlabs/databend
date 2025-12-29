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
use std::time::Instant;

use databend_common_exception::Result;
use log::info;

use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::DPhpyOptimizer;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::RuleID;

pub struct EliminateSelfJoinOptimizer {
    opt_ctx: Arc<OptimizerContext>,
}

impl EliminateSelfJoinOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self { opt_ctx }
    }
}

#[async_trait::async_trait]
impl Optimizer for EliminateSelfJoinOptimizer {
    fn name(&self) -> String {
        "EliminateSelfJoinOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        // `EagerAggregation` is used here as a speculative pre-rewrite to expose patterns that
        // `EliminateSelfJoin` can match. If no self-join is actually eliminated, we intentionally
        // return the original input plan to avoid keeping the eager-aggregation rewrite as a
        // standalone optimization.
        let start = Instant::now();
        static RULES_EAGER_AGGREGATION: &[RuleID] = &[RuleID::EagerAggregation];
        let optimizer = RecursiveRuleOptimizer::new(self.opt_ctx.clone(), RULES_EAGER_AGGREGATION);
        let s_expr_after_eager_aggregation = optimizer.optimize_sync(s_expr)?;

        static RULES_ELIMINATE_SELF_JOIN: &[RuleID] = &[RuleID::EliminateSelfJoin];
        let optimizer =
            RecursiveRuleOptimizer::new(self.opt_ctx.clone(), RULES_ELIMINATE_SELF_JOIN);
        let s_expr_after_eliminate_self_join =
            optimizer.optimize_sync(&s_expr_after_eager_aggregation)?;

        let duration = start.elapsed();

        if s_expr_after_eliminate_self_join == s_expr_after_eager_aggregation {
            return Ok(s_expr.clone());
        }

        // EliminateSelfJoinOptimizer should ideally run before Dphyp in the optimizer pipeline.
        // However, due to issues with the current EagerAggregation implementation, running it
        // before Dphyp causes TPC-H Q18 optimization to fail. Therefore, EliminateSelfJoinOptimizer
        // is placed after Dphyp, and we run Dphyp again here to ensure join reordering after
        // eliminating self-joins.
        let s_expr_after_dphyp = DPhpyOptimizer::new(self.opt_ctx.clone())
            .optimize_async(&s_expr_after_eliminate_self_join)
            .await?;

        info!("EliminateSelfJoinOptimizer: {}ms", duration.as_millis());

        Ok(s_expr_after_dphyp)
    }
}
