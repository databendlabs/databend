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
use super::optimize_group::OptimizeGroupTask;
use crate::IndexType;
use crate::optimizer::cost::Cost;
use crate::optimizer::cost::CostContext;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::DistributionEnforcer;
use crate::optimizer::ir::Enforcer;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::optimizers::cascades::CascadesOptimizer;
use crate::optimizer::optimizers::cascades::tasks::SharedCounter;
use crate::optimizer::optimizers::cascades::tasks::TaskManager;

#[derive(Clone, Copy, Debug)]
pub enum OptimizeExprState {
    Init,
    OptimizingChildren,
    OptimizedChildren,
    OptimizingSelf,
    OptimizedSelf,
    Finished,
}

#[derive(Clone, Copy, Debug)]
pub enum OptimizeExprEvent {
    OptimizingChildren,
    OptimizedChildren,
    OptimizingSelf,
    OptimizedSelf,
    Finish,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct OptimizeExprTask {
    #[educe(Debug(ignore))]
    pub ctx: Arc<dyn TableContext>,

    pub required_prop: RequiredProperty,
    pub children_required_props: Vec<RequiredProperty>,

    pub children_task_scheduled: bool,

    pub state: OptimizeExprState,

    pub group_index: IndexType,
    pub m_expr_index: IndexType,
    pub last_optimized_child_index: Option<IndexType>,

    /// Cost lower bound of the optimizing expression. Will be updated when optimizing
    /// children. If the cost lower bound is greater than the current best cost, we can
    /// prune the current expression.
    pub cost_lower_bound: Cost,

    pub ref_count: SharedCounter,
    pub parent: Option<SharedCounter>,
}

impl OptimizeExprTask {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        optimizer: &CascadesOptimizer,
        group_index: IndexType,
        m_expr_index: IndexType,
        required_prop: RequiredProperty,
        children_required_props: Vec<RequiredProperty>,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            state: OptimizeExprState::Init,
            group_index,
            m_expr_index,
            ref_count: SharedCounter::new(),
            parent: None,
            required_prop,
            children_required_props,
            children_task_scheduled: false,
            last_optimized_child_index: None,
            cost_lower_bound: optimizer.cost_model.compute_cost(
                &optimizer.memo,
                optimizer.memo.group(group_index)?.m_expr(m_expr_index)?,
            )?,
        })
    }

    pub fn with_parent(mut self, parent: SharedCounter) -> Self {
        self.parent = Some(parent);
        self
    }

    pub fn execute(
        mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<Option<Task>> {
        if matches!(self.state, OptimizeExprState::Finished) {
            return Ok(None);
        }
        self.transition(optimizer, scheduler)?;
        Ok(Some(Task::OptimizeExpr(self)))
    }

    fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<()> {
        let event = match self.state {
            OptimizeExprState::Init => OptimizeExprEvent::OptimizingChildren,
            OptimizeExprState::OptimizingChildren => {
                self.optimize_children(optimizer, scheduler)?
            }
            OptimizeExprState::OptimizedChildren => self.add_enforcers(optimizer)?,
            OptimizeExprState::OptimizingSelf => self.optimize_self(optimizer, scheduler)?,
            OptimizeExprState::OptimizedSelf => self.finish(optimizer)?,
            OptimizeExprState::Finished => Err(ErrorCode::Internal(
                "OptimizeExprTask should not be in Finished state",
            ))?,
        };

        match (self.state, event) {
            (OptimizeExprState::Init, OptimizeExprEvent::OptimizingChildren) => {
                self.state = OptimizeExprState::OptimizingChildren;
            }
            (OptimizeExprState::OptimizingChildren, OptimizeExprEvent::OptimizingChildren) => {}
            (OptimizeExprState::OptimizingChildren, OptimizeExprEvent::OptimizedChildren) => {
                self.state = OptimizeExprState::OptimizedChildren;
            }
            (OptimizeExprState::OptimizingChildren, OptimizeExprEvent::OptimizedSelf) => {
                self.state = OptimizeExprState::OptimizedSelf;
            }
            (OptimizeExprState::OptimizedChildren, OptimizeExprEvent::OptimizingSelf) => {
                self.state = OptimizeExprState::OptimizingSelf;
            }
            (OptimizeExprState::OptimizedChildren, OptimizeExprEvent::OptimizedSelf) => {
                self.state = OptimizeExprState::OptimizedSelf;
            }
            (OptimizeExprState::OptimizingSelf, OptimizeExprEvent::OptimizedSelf) => {
                self.state = OptimizeExprState::OptimizedSelf;
            }

            (OptimizeExprState::OptimizedSelf, OptimizeExprEvent::Finish) => {
                self.state = OptimizeExprState::Finished;
            }

            _ => Err(ErrorCode::Internal(format!(
                "Invalid transition from {:?} with event {:?}",
                self.state, event
            )))?,
        }

        Ok(())
    }

    fn optimize_children(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut TaskManager,
    ) -> Result<OptimizeExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;

        if m_expr.plan.physical().is_some_and(|op| op.is_exchange())
            && matches!(self.required_prop.distribution, Distribution::Any)
        {
            return Ok(OptimizeExprEvent::OptimizedSelf);
        }

        if m_expr.children.is_empty() {
            self.children_task_scheduled = true;
            return Ok(OptimizeExprEvent::OptimizedChildren);
        }

        // Check if the cost lower bound is already greater than the current best cost,
        // if so, we can prune the current expression.
        if let Some(ccx) = optimizer
            .memo
            .group(self.group_index)?
            .best_prop(&self.required_prop)
        {
            if let Some(last_optimized_child_index) = self.last_optimized_child_index {
                let child_index = last_optimized_child_index - 1;
                if let Some(c) = optimizer
                    .memo
                    .group(m_expr.children[child_index])?
                    .best_prop(&self.children_required_props[child_index])
                {
                    self.cost_lower_bound += c.cost;
                }
            }

            if ccx.cost < self.cost_lower_bound {
                scheduler.stat.optimize_group_expr_prune_count += 1;
                self.children_task_scheduled = true;
                return Ok(OptimizeExprEvent::OptimizedSelf);
            }
        }

        if !self.children_task_scheduled {
            let child_index = self.last_optimized_child_index.unwrap_or(0);
            let required_prop = self
                .children_required_props
                .get(child_index)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Cannot find required property for child: {}",
                        child_index
                    ))
                })?
                .clone();

            let group = optimizer.memo.group(m_expr.children[child_index])?;
            if group.best_prop(&required_prop).is_none() {
                let child_group_index = group.group_index;
                // An in-flight task for the same `(group, required_prop)` computes exactly
                // the result we need, so wait for it instead of scheduling a duplicate.
                //
                // Enforcer expressions (e.g. `Exchange`) have their own group as child and
                // rely on `owner_expr` to skip themselves; those requests keep a private
                // task so that the skip stays tied to the requesting expression.
                let shareable = child_group_index != self.group_index;
                let attached = shareable
                    && scheduler.attach_optimize_group_waiter(
                        child_group_index,
                        &required_prop,
                        &self.ref_count,
                    );
                if !attached {
                    let task = OptimizeGroupTask::new(
                        self.ctx.clone(),
                        Some((self.group_index, self.m_expr_index)),
                        child_group_index,
                        required_prop.clone(),
                    )
                    .with_parent(self.ref_count.clone());
                    if shareable {
                        scheduler.register_optimize_group(
                            child_group_index,
                            required_prop.clone(),
                            task.waiters.clone(),
                        );
                    }
                    scheduler.add_task(Task::OptimizeGroup(task));
                }
            }

            self.last_optimized_child_index = Some(child_index + 1);

            if self
                .last_optimized_child_index
                .map(|i| i == m_expr.children.len())
                .unwrap_or(false)
            {
                self.children_task_scheduled = true;
            }

            Ok(OptimizeExprEvent::OptimizingChildren)
        } else {
            Ok(OptimizeExprEvent::OptimizedChildren)
        }
    }

    fn optimize_self(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        _scheduler: &mut TaskManager,
    ) -> Result<OptimizeExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;
        let mut cost = Cost::from(0);

        let mut children_best_props = Vec::with_capacity(self.children_required_props.len());

        for (child, required_prop) in m_expr
            .children
            .iter()
            .zip(self.children_required_props.iter())
        {
            let group = optimizer.memo.group(*child)?;
            let cost_context = group.best_prop(required_prop).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Cannot find best property for group: {}",
                    group.group_index
                ))
            })?;
            children_best_props.push(cost_context.physical_prop.clone());
            cost += cost_context.cost;
        }

        let op_cost = optimizer.cost_model.compute_cost(&optimizer.memo, m_expr)?;
        cost += op_cost;

        let rel_expr = RelExpr::with_opt_context(m_expr, &optimizer.memo, &children_best_props);

        let cost_context = CostContext {
            cost,
            group_index: m_expr.group_index,
            expr_index: m_expr.index,
            children_required_props: self.children_required_props.clone(),
            physical_prop: rel_expr.derive_physical_prop()?,
        };

        let group = optimizer.memo.group_mut(self.group_index)?;
        group.update_best_cost(&self.required_prop, cost_context);

        Ok(OptimizeExprEvent::OptimizedSelf)
    }

    /// Add enforcers to the group according to the required property.
    fn add_enforcers(&mut self, optimizer: &mut CascadesOptimizer) -> Result<OptimizeExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;
        let children_best_props = match self
            .children_required_props
            .iter()
            .zip(m_expr.children.iter())
            .map(|(prop, group_index)| {
                let group = optimizer.memo.group(*group_index)?;
                let physical_prop = group
                    .best_prop(prop)
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Cannot find best property for group: {}",
                            group.group_index
                        ))
                    })?
                    .physical_prop
                    .clone();
                Ok(physical_prop)
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(props) => props,
            Err(_) => {
                // If any child group does not have a best property, we cannot add enforcers.
                // This may happen when the child cannot be optimized in the current context.
                return Ok(OptimizeExprEvent::OptimizedSelf);
            }
        };

        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;
        let rel_expr = RelExpr::with_opt_context(m_expr, &optimizer.memo, &children_best_props);
        let physical_prop = rel_expr.derive_physical_prop()?;

        // Exchanges repartition copies; they cannot turn replicated output into
        // single-production output. Reject this candidate and let the group search
        // select an implementation with a concrete non-broadcast distribution.
        if self.required_prop.distribution == Distribution::NonBroadcast
            && !self.required_prop.satisfied_by(&physical_prop)
        {
            return Ok(OptimizeExprEvent::OptimizedSelf);
        }

        let should_enforce = {
            let mut should_enforce = true;

            if optimizer.enforce_distribution()
                && physical_prop.distribution == Distribution::Serial
                && !matches!(
                    self.required_prop.distribution,
                    Distribution::Serial | Distribution::Any | Distribution::NonBroadcast
                )
            {
                should_enforce = false;
            }

            if optimizer.enforce_distribution()
                && children_best_props
                    .iter()
                    .any(|prop| prop.distribution == Distribution::Serial)
                && !children_best_props
                    .iter()
                    .all(|prop| prop.distribution == Distribution::Serial)
            {
                should_enforce = false;
            }

            should_enforce
        };

        // Sometimes we cannot enforce the required property and we cannot
        // optimize the expression in this situation.
        if !should_enforce {
            return Ok(OptimizeExprEvent::OptimizedSelf);
        }

        let mut enforcers: Vec<Box<dyn Enforcer>> = Vec::new();

        // Enforcers of distribution.
        let dist = self.required_prop.distribution.clone();
        let enforcer = Box::new(DistributionEnforcer::from(dist));
        if enforcer.check_enforce(&physical_prop) {
            enforcers.push(enforcer);
        }

        if enforcers.is_empty() {
            return Ok(OptimizeExprEvent::OptimizingSelf);
        }

        for enforcer in enforcers.iter() {
            optimizer
                .memo
                .insert_physical(self.group_index, enforcer.enforce()?, vec![
                    self.group_index,
                ])?;
        }

        Ok(OptimizeExprEvent::OptimizedSelf)
    }

    fn finish(&mut self, _optimizer: &mut CascadesOptimizer) -> Result<OptimizeExprEvent> {
        Ok(OptimizeExprEvent::Finish)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::table_context::TableContextSettings;
    use databend_common_sql_test_support::LiteTableContext;

    use super::*;
    use crate::MetadataRef;
    use crate::optimizer::OptimizerContext;
    use crate::optimizer::ir::PhysicalProperty;
    use crate::optimizer::ir::SExpr;
    use crate::plans::DummyTableScan;
    use crate::plans::Exchange;
    use crate::plans::PhysicalRelOperator;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn non_broadcast_search_rejects_replicated_candidate() -> Result<()> {
        // Isolate LiteTableContext's process-wide singleton initialization, as in
        // the other scheduler tests. This tests candidate selection, not a rewrite.
        const CHILD_PROCESS: &str = "DATABEND_NON_BROADCAST_SCHEDULER_TEST";
        if std::env::var_os(CHILD_PROCESS).is_none() {
            let name = concat!(
                module_path!(),
                "::non_broadcast_search_rejects_replicated_candidate"
            );
            let output = std::process::Command::new(std::env::current_exe()?)
                .env(CHILD_PROCESS, "1")
                .args(["--exact", name.split_once("::").unwrap().1, "--nocapture"])
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "isolated distribution test failed:\n{stdout}\n{}",
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
        opt_ctx.set_enable_distributed_optimization(true);
        let mut optimizer = CascadesOptimizer::new(opt_ctx)?;
        optimizer
            .memo
            .init(SExpr::create_leaf(DummyTableScan::new()))?;
        let group = optimizer.memo.root().unwrap().group_index;
        optimizer.memo.implement_group(group)?;
        let required = RequiredProperty {
            distribution: Distribution::NonBroadcast,
        };
        let mut task = OptimizeExprTask::new(ctx, &optimizer, group, 1, required.clone(), vec![])?;
        assert!(matches!(
            task.add_enforcers(&mut optimizer)?,
            OptimizeExprEvent::OptimizingSelf
        ));
        task.optimize_self(&mut optimizer, &mut TaskManager::new())?;
        let selected = optimizer
            .memo
            .group(group)?
            .best_prop(&required)
            .unwrap()
            .clone();
        assert_eq!(selected.physical_prop.distribution, Distribution::Serial);

        let any = RequiredProperty::default();
        optimizer
            .memo
            .group_mut(group)?
            .update_best_cost(&any, CostContext {
                physical_prop: PhysicalProperty {
                    distribution: Distribution::Serial,
                },
                ..selected.clone()
            });
        optimizer
            .memo
            .insert_physical(group, Exchange::Broadcast, vec![group])?;
        let broadcast_index = optimizer.memo.group(group)?.num_exprs() - 1;
        task.m_expr_index = broadcast_index;
        task.children_required_props = vec![any];
        let before = optimizer.memo.group(group)?.num_exprs();
        assert!(matches!(
            task.add_enforcers(&mut optimizer)?,
            OptimizeExprEvent::OptimizedSelf
        ));
        assert_eq!(optimizer.memo.group(group)?.num_exprs(), before);
        let best = optimizer.memo.group(group)?.best_prop(&required).unwrap();
        assert_eq!(best.expr_index, selected.expr_index);
        assert!(!matches!(
            optimizer
                .memo
                .group(group)?
                .m_expr(best.expr_index)?
                .plan
                .physical(),
            Some(PhysicalRelOperator::Exchange(_))
        ));
        Ok(())
    }
}
