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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use super::Task;
use super::optimize_expr::OptimizeExprTask;
use crate::IndexType;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::optimizers::cascades::CascadesOptimizer;
use crate::optimizer::optimizers::cascades::tasks::ExploreGroupTask;
use crate::optimizer::optimizers::cascades::tasks::SharedCounter;
use crate::optimizer::optimizers::cascades::tasks::TaskManager;
use crate::plans::Operator;

#[derive(Clone, Copy, Debug)]
pub enum OptimizeGroupState {
    Init,
    Explored,
    Optimized,
}

#[derive(Clone, Copy, Debug)]
pub enum OptimizeGroupEvent {
    Exploring,
    Explored,
    Optimizing,
    Optimized,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct OptimizeGroupTask {
    #[educe(Debug(ignore))]
    pub ctx: Arc<dyn TableContext>,

    pub required_prop: RequiredProperty,

    pub state: OptimizeGroupState,
    pub owner_expr: Option<(IndexType, IndexType)>,
    pub group_index: IndexType,
    pub last_optimized_expr_index: Option<IndexType>,

    pub ref_count: SharedCounter,
    pub parent: Option<SharedCounter>,
}

impl OptimizeGroupTask {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        owner_expr: Option<(IndexType, IndexType)>,
        group_index: IndexType,
        required_prop: RequiredProperty,
    ) -> Self {
        Self {
            ctx,
            required_prop,
            state: OptimizeGroupState::Init,
            group_index,
            last_optimized_expr_index: None,
            ref_count: SharedCounter::new(),
            parent: None,
            owner_expr,
        }
    }

    pub fn with_parent(self, parent: SharedCounter) -> Self {
        let mut task = self;
        task.parent = Some(parent);
        task
    }

    pub fn execute(
        mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<Option<Task>> {
        if matches!(self.state, OptimizeGroupState::Optimized) {
            return Ok(None);
        }
        self.transition(optimizer, scheduler)?;
        Ok(Some(Task::OptimizeGroup(self)))
    }

    fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<()> {
        let event = match self.state {
            OptimizeGroupState::Init => self.explore_group(optimizer, scheduler)?,
            OptimizeGroupState::Explored => self.optimize_group(optimizer, scheduler)?,
            OptimizeGroupState::Optimized => Err(ErrorCode::Internal(
                "Invalid transition from Optimized state".to_string(),
            ))?,
        };

        match (self.state, event) {
            (OptimizeGroupState::Init, OptimizeGroupEvent::Exploring) => {}
            (OptimizeGroupState::Init, OptimizeGroupEvent::Explored) => {
                self.state = OptimizeGroupState::Explored;
            }
            (OptimizeGroupState::Explored, OptimizeGroupEvent::Optimizing) => {}
            (OptimizeGroupState::Explored, OptimizeGroupEvent::Optimized) => {
                self.state = OptimizeGroupState::Optimized;
            }
            _ => Err(ErrorCode::Internal(format!(
                "Invalid transition from {:?} with {:?}",
                self.state, event
            )))?,
        }

        Ok(())
    }

    fn explore_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<OptimizeGroupEvent> {
        let group = optimizer.memo.group(self.group_index)?;
        if !group.state.explored() {
            let task = ExploreGroupTask::new(self.ctx.clone(), group.group_index)
                .with_parent(self.ref_count.clone());
            scheduler.add_task(Task::ExploreGroup(task));
            Ok(OptimizeGroupEvent::Exploring)
        } else {
            Ok(OptimizeGroupEvent::Explored)
        }
    }

    fn optimize_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<OptimizeGroupEvent> {
        // Complete property-independent implementation enumeration before using
        // a cached best choice. New candidates invalidate both the choice and our
        // scheduling cursor; every physical root must participate in costing.
        if optimizer.memo.implement_group(self.group_index)? {
            self.last_optimized_expr_index = None;
        }
        let group = optimizer.memo.group_mut(self.group_index)?;

        if group.best_prop(&self.required_prop).is_some() {
            scheduler.stat.optimize_group_prune_count += 1;
            return Ok(OptimizeGroupEvent::Optimized);
        }

        // Check if there is new added `MExpr`s.
        let start_index = self.last_optimized_expr_index.unwrap_or_default();
        if start_index == group.num_exprs() {
            return Ok(OptimizeGroupEvent::Optimized);
        }

        let m_exprs = group.m_exprs[start_index..].to_vec();

        self.last_optimized_expr_index = Some(group.num_exprs());

        for m_expr in m_exprs.iter() {
            if Some((self.group_index, m_expr.index)) == self.owner_expr || m_expr.plan.is_logical()
            {
                continue;
            }

            let rel_expr = RelExpr::with_m_expr(m_expr, &optimizer.memo);
            let children_required_props = if optimizer.enforce_distribution() {
                rel_expr.compute_required_prop_children(self.ctx.clone(), &self.required_prop)?
            } else {
                vec![vec![RequiredProperty::default(); m_expr.plan.arity()]]
            };

            for required_props in children_required_props {
                let task = OptimizeExprTask::new(
                    self.ctx.clone(),
                    optimizer,
                    self.group_index,
                    m_expr.index,
                    self.required_prop.clone(),
                    required_props,
                )?
                .with_parent(self.ref_count.clone());
                scheduler.add_task(Task::OptimizeExpr(task));
            }
        }

        Ok(OptimizeGroupEvent::Optimizing)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use databend_common_catalog::table_context::TableContextSettings;
    use databend_common_sql_test_support::LiteTableContext;

    use super::*;
    use crate::MetadataRef;
    use crate::optimizer::OptimizerContext;
    use crate::optimizer::cost::Cost;
    use crate::optimizer::cost::CostContext;
    use crate::optimizer::cost::CostModel;
    use crate::optimizer::ir::GroupState;
    use crate::optimizer::ir::MExpr;
    use crate::optimizer::ir::Memo;
    use crate::optimizer::ir::PhysicalProperty;
    use crate::optimizer::ir::SExpr;
    use crate::optimizer::optimizers::cascades::cost::DefaultCostModel;
    use crate::plans::DummyTableScan;
    use crate::plans::EvalScalar;
    use crate::plans::Limit;
    use crate::plans::PhysicalRelOperator;

    struct RecordingCostModel {
        inner: DefaultCostModel,
        visited: Arc<Mutex<Vec<(IndexType, IndexType)>>>,
    }

    impl CostModel for RecordingCostModel {
        fn compute_cost(&self, memo: &Memo, expr: &MExpr) -> Result<Cost> {
            assert!(
                expr.plan.is_physical(),
                "logical expression reached costing"
            );
            self.visited
                .lock()
                .unwrap()
                .push((expr.group_index, expr.index));
            let cost = self.inner.compute_cost(memo, expr)?;
            // Test-only cost preference between equivalent identity operators.
            if expr.plan.physical().unwrap().is_eval_scalar() {
                Ok(Cost(cost.0 / 2.0))
            } else {
                Ok(cost)
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn new_implementations_reset_choices_and_all_roots_are_costed() -> Result<()> {
        // Scheduling contract, not rewrite-rule evidence. Both LIMIT ALL and
        // empty EvalScalar are identities here. No production strategy is added.
        // LiteTableContext initializes process-wide singleton dispatch. Most SQL
        // unit tests deliberately run without it, so isolate this scheduler test
        // rather than changing their initialization requirements.
        const CHILD_PROCESS: &str = "DATABEND_IMPLEMENTATION_SCHEDULER_TEST";
        if std::env::var_os(CHILD_PROCESS).is_none() {
            let name = concat!(
                module_path!(),
                "::new_implementations_reset_choices_and_all_roots_are_costed"
            );
            let output = std::process::Command::new(std::env::current_exe()?)
                .env(CHILD_PROCESS, "1")
                .args(["--exact", name.split_once("::").unwrap().1, "--nocapture"])
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "isolated scheduler test failed:\n{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return Ok(());
        }
        let ctx = LiteTableContext::create().await?;
        let opt_ctx = OptimizerContext::new(
            ctx.clone(),
            MetadataRef::default(),
            ctx.get_function_context()?,
        );
        let mut optimizer = CascadesOptimizer::new(opt_ctx)?;
        optimizer.memo.init(SExpr::create_unary(
            Limit {
                before_exchange: false,
                limit: None,
                offset: 0,
                lazy_columns: Default::default(),
            },
            SExpr::create_leaf(DummyTableScan::new()),
        ))?;
        let root = optimizer.memo.root.unwrap();
        let input = optimizer.memo.group(root)?.m_exprs[0].children[0];
        optimizer.memo.set_group_state(root, GroupState::Explored)?;
        optimizer.memo.insert_physical(
            root,
            PhysicalRelOperator::EvalScalar(EvalScalar { items: vec![] }),
            vec![input],
        )?;
        let required = RequiredProperty::default();
        // An incomplete candidate set must not make an old cached choice or
        // cursor hide the default implementation that is still pending.
        optimizer
            .memo
            .group_mut(root)?
            .update_best_cost(&required, CostContext {
                group_index: root,
                expr_index: 1,
                cost: Cost(0.0),
                physical_prop: PhysicalProperty::default(),
                children_required_props: vec![],
            });
        let mut task = OptimizeGroupTask::new(ctx.clone(), None, root, required.clone());
        task.last_optimized_expr_index = Some(optimizer.memo.group(root)?.num_exprs());
        let visited = Arc::new(Mutex::new(vec![]));
        optimizer.cost_model = Box::new(RecordingCostModel {
            inner: DefaultCostModel::new(ctx)?,
            visited: visited.clone(),
        });
        let mut scheduler = TaskManager::new();
        scheduler.add_task(Task::OptimizeGroup(task));
        scheduler.run(&mut optimizer)?;
        let visited = visited.lock().unwrap();
        assert!(
            visited.contains(&(root, 1)),
            "alternative implementation was not costed"
        );
        assert!(
            visited.contains(&(root, 2)),
            "default implementation was not costed"
        );
        let best = optimizer.memo.group(root)?.best_prop(&required).unwrap();
        assert_eq!(best.expr_index, 1);
        assert_eq!(best.cost, Cost(0.5));
        Ok(())
    }
}
