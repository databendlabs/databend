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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;
use log::info;

use crate::IndexType;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::cost::CostModel;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::cascades::cost::DefaultCostModel;
use crate::optimizer::optimizers::cascades::rule::StrategyFactory;
use crate::optimizer::optimizers::cascades::tasks::DEFAULT_TASK_LIMIT;
use crate::optimizer::optimizers::cascades::tasks::OptimizeGroupTask;
use crate::optimizer::optimizers::cascades::tasks::Task;
use crate::optimizer::optimizers::cascades::tasks::TaskManager;
use crate::optimizer::optimizers::distributed::DistributedOptimizer;
use crate::optimizer::optimizers::distributed::SortAndLimitPushDownOptimizer;
use crate::optimizer::optimizers::rule::RuleSet;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::RelOperator;

/// A cascades-style search engine to enumerate possible alternations of a relational expression and
/// find the optimal one.
pub struct CascadesOptimizer {
    pub(crate) opt_ctx: Arc<OptimizerContext>,
    pub(crate) memo: Memo,
    pub(crate) cost_model: Box<dyn CostModel>,
    pub(crate) explore_rule_set: RuleSet,
}

impl CascadesOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Result<Self> {
        let table_ctx = opt_ctx.get_table_ctx();
        let settings = table_ctx.get_settings();

        // Create a default rule set initially
        // This is just a placeholder - the actual rule set will be determined at runtime
        // in the optimize_with_cascade method based on current flag values
        let explore_rule_set = RuleSet::create();

        // Build cost model
        let cost_model = Box::new(
            DefaultCostModel::new(table_ctx.clone())?
                .with_cluster_peers(table_ctx.get_cluster().nodes.len())
                .with_degree_of_parallelism(settings.get_max_threads()? as usize),
        );

        // Create optimizer
        Ok(Self {
            opt_ctx,
            memo: Memo::create(),
            cost_model,
            explore_rule_set,
        })
    }

    pub(crate) fn enforce_distribution(&self) -> bool {
        self.opt_ctx.get_enable_distributed_optimization()
    }

    fn init(&mut self, expression: SExpr) -> Result<()> {
        self.memo.init(expression)?;

        Ok(())
    }

    #[recursive::recursive]
    pub fn optimize_sync(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let opt_ctx = self.opt_ctx.clone();

        // Try to optimize using the internal optimizer
        let result = self.optimize_internal(s_expr.clone());

        // Process different cases based on the result
        let mut optimized_expr = match result {
            Ok(expr) => {
                // After successful optimization, apply sort and limit push down if distributed optimization is enabled
                if opt_ctx.get_enable_distributed_optimization() {
                    let sort_and_limit_optimizer = SortAndLimitPushDownOptimizer::create();
                    sort_and_limit_optimizer.optimize(&expr)?
                } else {
                    expr
                }
            }

            Err(e) => {
                // Optimization failed, log the error and fall back to heuristic optimizer
                info!(
                    "CascadesOptimizer failed, fallback to heuristic optimizer: {}",
                    e
                );

                // If distributed optimization is enabled, use the distributed optimizer
                if opt_ctx.get_enable_distributed_optimization() {
                    let distributed_optimizer = DistributedOptimizer::new(opt_ctx);
                    distributed_optimizer.optimize(&s_expr)?
                } else {
                    // Otherwise return the original expression
                    s_expr.clone()
                }
            }
        };

        optimized_expr = Self::remove_exchanges_for_serial_sequence(optimized_expr)?;

        Ok(optimized_expr)
    }

    fn remove_exchanges_for_serial_sequence(s_expr: SExpr) -> Result<SExpr> {
        if Self::has_sequence_with_serial_left_child(&s_expr)? {
            Self::remove_all_exchanges(s_expr)
        } else {
            Ok(s_expr)
        }
    }

    fn has_sequence_with_serial_left_child(s_expr: &SExpr) -> Result<bool> {
        if let RelOperator::Sequence(_) = s_expr.plan.as_ref() {
            let left_child = s_expr.left_child();
            let rel_expr = RelExpr::with_s_expr(left_child);
            let physical_prop = rel_expr.derive_physical_prop()?;

            if physical_prop.distribution == Distribution::Serial {
                return Ok(true);
            }
        }

        for child in s_expr.children() {
            if Self::has_sequence_with_serial_left_child(child)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn remove_all_exchanges(s_expr: SExpr) -> Result<SExpr> {
        if let RelOperator::Exchange(_) = s_expr.plan.as_ref() {
            return Self::remove_all_exchanges(s_expr.unary_child().clone());
        }

        let mut new_children = Vec::new();
        for child in s_expr.children() {
            let processed_child = Self::remove_all_exchanges(child.clone())?;
            new_children.push(Arc::new(processed_child));
        }

        let result = s_expr.replace_children(new_children);
        Ok(result)
    }

    fn optimize_internal(&mut self, s_expr: SExpr) -> Result<SExpr> {
        // Update rule set based on current flags
        // This ensures we use the most up-to-date flag values, regardless of when the optimizer was created
        let table_ctx = self.opt_ctx.get_table_ctx();
        let settings = table_ctx.get_settings();

        // Determine the appropriate rule set at runtime
        // This is critical for pipeline execution where optimizers are created before all flags are set
        if settings.get_enable_cbo()? {
            // Check if DPhyp has already performed join reordering
            // The "dphyp_optimized" flag is set by the DPhyp optimizer when it runs
            // In pipeline execution, this flag will be set before CascadesOptimizer runs
            let use_optimized_join_order = self.opt_ctx.get_flag("dphyp_optimized")
                || unsafe { settings.get_disable_join_reorder()? };

            let rule_strategy = StrategyFactory::create_strategy(use_optimized_join_order);
            self.explore_rule_set = rule_strategy.create_rule_set();
        }

        self.init(s_expr)?;

        debug!("Init memo:\n{}", self.memo.display()?);

        let root_index = self
            .memo
            .root()
            .ok_or_else(|| ErrorCode::Internal("Root group cannot be None after initialization"))?
            .group_index;

        let root_required_prop = if self.enforce_distribution() {
            RequiredProperty {
                distribution: Distribution::Serial,
            }
        } else {
            Default::default()
        };

        let root_task = OptimizeGroupTask::new(
            self.opt_ctx.get_table_ctx().clone(),
            None,
            root_index,
            root_required_prop.clone(),
        );

        let task_limit = if self
            .opt_ctx
            .get_table_ctx()
            .get_settings()
            .get_enable_cbo()?
        {
            DEFAULT_TASK_LIMIT
        } else {
            0
        };

        let mut scheduler = TaskManager::new().with_task_limit(task_limit);
        scheduler.add_task(Task::OptimizeGroup(root_task));
        scheduler.run(self)?;

        debug!("Memo:\n{}", self.memo.display()?);

        self.find_best_plan(root_index, &root_required_prop)
    }

    pub(crate) fn insert_from_transform_state(
        &mut self,
        group_index: IndexType,
        state: TransformResult,
    ) -> Result<()> {
        for result in state.results() {
            self.insert_expression(group_index, result)?;
        }

        Ok(())
    }

    /// Insert a new `SExpr` into the memo. This will recursively insert all of its children.
    /// When inserting a new expression, we will first check if it is already in the memo. If it is,
    /// we will return the existing group index. Otherwise, we will create a new group and return
    /// its index.
    /// Currently, we can only check if the inserted expression is already in the memo by its
    /// `SExpr::original_group` field, which is set by the `PatternExtractor` when extracting
    /// candidates from memo. But sometimes, it's possible to insert the generated expression
    /// into a existed group, we'd better find a way to do this in the future to reduce duplicated
    /// groups.
    fn insert_expression(&mut self, group_index: IndexType, expression: &SExpr) -> Result<()> {
        self.memo.insert(Some(group_index), expression.clone())?;

        Ok(())
    }

    #[recursive::recursive]
    fn find_best_plan(
        &self,
        group_index: IndexType,
        required_property: &RequiredProperty,
    ) -> Result<SExpr> {
        let group = self.memo.group(group_index)?;
        let cost_context = group.best_prop(required_property).ok_or_else(|| {
            ErrorCode::Internal(format!("Cannot find best cost of group: {group_index}",))
        })?;

        let m_expr = group.m_exprs.get(cost_context.expr_index).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot find best expression of group: {group_index}"
            ))
        })?;

        let children = m_expr
            .children
            .iter()
            .zip(cost_context.children_required_props.iter())
            .map(|(index, required_prop)| Ok(Arc::new(self.find_best_plan(*index, required_prop)?)))
            .collect::<Result<Vec<_>>>()?;

        let result = SExpr::create(m_expr.plan.clone(), children, None, None, None);

        Ok(result)
    }
}

#[async_trait::async_trait]
impl Optimizer for CascadesOptimizer {
    fn name(&self) -> String {
        "CascadesOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr.clone())
    }

    fn memo(&self) -> Option<&Memo> {
        Some(&self.memo)
    }
}
