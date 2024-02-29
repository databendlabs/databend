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

use super::optimize_group::OptimizeGroupTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::cost::Cost;
use crate::optimizer::cost::CostContext;
use crate::optimizer::extract::Matcher;
use crate::optimizer::Distribution;
use crate::optimizer::DistributionEnforcer;
use crate::optimizer::Enforcer;
use crate::optimizer::PatternExtractor;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SExpr;
use crate::plans::RelOperator;
use crate::IndexType;

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
        scheduler: &mut Scheduler,
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
        scheduler: &mut Scheduler,
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
        scheduler: &mut Scheduler,
    ) -> Result<OptimizeExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;

        if matches!(m_expr.plan.as_ref(), RelOperator::Exchange(_),)
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
                let task = OptimizeGroupTask::new(
                    self.ctx.clone(),
                    Some((self.group_index, self.m_expr_index)),
                    group.group_index,
                    required_prop.clone(),
                )
                .with_parent(self.ref_count.clone());
                scheduler.add_task(Task::OptimizeGroup(task));
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
        _scheduler: &mut Scheduler,
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

        let should_enforce = {
            let mut should_enforce = true;

            if optimizer.enforce_distribution
                && physical_prop.distribution == Distribution::Serial
                && !matches!(
                    self.required_prop.distribution,
                    Distribution::Serial | Distribution::Any
                )
            {
                should_enforce = false;
            }

            if optimizer.enforce_distribution
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

        let mut extractor = PatternExtractor::new();
        let enforcer_child = Arc::new(
            extractor
                .extract(&optimizer.memo, m_expr, &Matcher::Leaf)?
                .pop()
                .ok_or_else(|| {
                    ErrorCode::Internal(format!("Cannot find child of m_expr: {:?}", m_expr.plan))
                })?,
        );

        for enforcer in enforcers.iter() {
            let operator = enforcer.enforce()?;
            let s_expr = SExpr::create_unary(Arc::new(operator), enforcer_child.clone());
            optimizer.memo.insert(Some(self.group_index), s_expr)?;
        }

        Ok(OptimizeExprEvent::OptimizedSelf)
    }

    fn finish(&mut self, _optimizer: &mut CascadesOptimizer) -> Result<OptimizeExprEvent> {
        Ok(OptimizeExprEvent::Finish)
    }
}
