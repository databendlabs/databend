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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use super::sort_and_limit::SortAndLimitPushDownOptimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PropertyEnforcer;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::SExpr;
use crate::plans::Exchange;

/// DistributedOptimizer optimizes a query plan for distributed execution.
/// It enforces distribution properties and applies sort/limit push down optimizations.
///
/// TODO(leiysky): Consider deprecating this in favor of cascades planner for distributed optimization.
pub struct DistributedOptimizer {
    ctx: Arc<dyn TableContext>,
    sort_limit_optimizer: SortAndLimitPushDownOptimizer,
}

impl DistributedOptimizer {
    /// Create a new DistributedOptimizer
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self {
            ctx: opt_ctx.get_table_ctx(),
            sort_limit_optimizer: SortAndLimitPushDownOptimizer::create(),
        }
    }

    /// Optimize the given SExpr for distributed execution
    #[stacksafe::stacksafe]
    pub fn optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        // Step 1: Set the initial distribution requirement (Any)
        let required = RequiredProperty {
            distribution: Distribution::Any,
        };

        // Step 2: Enforce the property
        let enforcer = PropertyEnforcer::new(self.ctx.clone());
        let result = enforcer.require_property(&required, s_expr)?;

        // Step 3: Apply the sort and limit push down optimization
        let mut result = self.sort_limit_optimizer.optimize(&result)?;

        // Step 4: Check if the result satisfies the required distribution property
        let rel_expr = RelExpr::with_s_expr(&result);
        let physical_prop = rel_expr.derive_physical_prop()?;
        let root_required = RequiredProperty {
            distribution: Distribution::Serial,
        };

        // Step 5: If not satisfied, manually enforce serial distribution
        if !root_required.satisfied_by(&physical_prop) {
            // Add an Exchange::Merge operator
            result = SExpr::create_unary(Arc::new(Exchange::Merge.into()), Arc::new(result));
        }

        Ok(result)
    }
}
