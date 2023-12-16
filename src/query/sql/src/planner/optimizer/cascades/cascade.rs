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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::debug;
use log::info;

use super::explore_rules::get_explore_rule_set;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::OptimizeGroupTask;
use crate::optimizer::cascades::tasks::Task;
use crate::optimizer::cost::CostContext;
use crate::optimizer::cost::CostModel;
use crate::optimizer::cost::DefaultCostModel;
use crate::optimizer::format::display_memo;
use crate::optimizer::memo::Memo;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleSet;
use crate::optimizer::SExpr;
use crate::IndexType;
use crate::MetadataRef;

/// A cascades-style search engine to enumerate possible alternations of a relational expression and
/// find the optimal one.
pub struct CascadesOptimizer {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) memo: Memo,
    pub(crate) cost_model: Box<dyn CostModel>,
    /// group index -> best cost context
    pub(crate) best_cost_map: HashMap<IndexType, CostContext>,
    pub(crate) explore_rule_set: RuleSet,
    pub(crate) metadata: MetadataRef,
}

impl CascadesOptimizer {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        metadata: MetadataRef,
        mut optimized: bool,
    ) -> Result<Self> {
        let explore_rule_set = if ctx.get_settings().get_enable_cbo()? {
            if unsafe { ctx.get_settings().get_disable_join_reorder()? } {
                optimized = true;
            }
            get_explore_rule_set(optimized)
        } else {
            RuleSet::create()
        };
        Ok(CascadesOptimizer {
            ctx,
            memo: Memo::create(),
            cost_model: Box::new(DefaultCostModel),
            best_cost_map: HashMap::new(),
            explore_rule_set,
            metadata,
        })
    }

    fn init(&mut self, expression: SExpr) -> Result<()> {
        self.memo.init(expression)?;

        Ok(())
    }

    pub fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.init(s_expr)?;

        let root_index = self
            .memo
            .root()
            .ok_or_else(|| ErrorCode::Internal("Root group cannot be None after initialization"))?
            .group_index;

        let root_task = OptimizeGroupTask::new(self.ctx.clone(), root_index);

        let start_time = std::time::Instant::now();
        let mut num_task_apply_rule = 0;
        let mut scheduler = Scheduler::new().with_callback(|task| {
            if let Task::ApplyRule(_) = task {
                num_task_apply_rule += 1;
            }
        });
        scheduler.add_task(Task::OptimizeGroup(root_task));
        scheduler.run(self)?;

        let scheduled_task_count = scheduler.scheduled_task_count();
        drop(scheduler);
        let elapsed = start_time.elapsed();

        info!(
            "optimizer stats - total task number: {:#?}, total execution time: {:.3}ms, average execution time: {:.3}ms, apply rule task number: {:#?}",
            scheduled_task_count,
            elapsed.as_millis() as f64,
            elapsed.as_millis() as f64 / scheduled_task_count as f64,
            num_task_apply_rule,
        );

        debug!("Memo:\n{}", display_memo(&self.memo, &self.best_cost_map)?);

        self.find_optimal_plan(root_index)
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

    fn find_optimal_plan(&self, group_index: IndexType) -> Result<SExpr> {
        let group = self.memo.group(group_index)?;
        let cost_context = self.best_cost_map.get(&group_index).ok_or_else(|| {
            ErrorCode::Internal(format!("Cannot find CostContext of group: {group_index}"))
        })?;

        let m_expr = group.m_exprs.get(cost_context.expr_index).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot find best expression of group: {group_index}"
            ))
        })?;

        let children = m_expr
            .children
            .iter()
            .map(|index| Ok(Arc::new(self.find_optimal_plan(*index)?)))
            .collect::<Result<Vec<_>>>()?;

        let result = SExpr::create(m_expr.plan.clone(), children, None, None, None);

        Ok(result)
    }
}
