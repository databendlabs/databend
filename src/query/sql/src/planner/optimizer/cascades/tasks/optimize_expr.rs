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

use std::collections::hash_map::Entry;
use std::rc::Rc;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use educe::Educe;

use super::optimize_group::OptimizeGroupTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::cost::Cost;
use crate::optimizer::cost::CostContext;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SExpr;
use crate::plans::Operator;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::IndexType;

#[derive(Clone, Copy, Debug)]
pub enum OptimizeExprState {
    Init,
    OptimizedChildren,
    OptimizedSelf,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub enum OptimizeExprEvent {
    OptimizingChildren,
    OptimizedChildren,
    OptimizingSelf,
    OptimizedSelf,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct OptimizeExprTask {
    #[educe(Debug(ignore))]
    pub ctx: Arc<dyn TableContext>,

    pub state: OptimizeExprState,

    pub required: RequiredProperty,
    pub group_index: IndexType,
    pub m_expr_index: IndexType,
    pub children_props: Vec<Vec<RequiredProperty>>,

    pub ref_count: Rc<SharedCounter>,
    pub parent: Option<Rc<SharedCounter>>,
}

impl OptimizeExprTask {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        group_index: IndexType,
        m_expr_index: IndexType,
        required: RequiredProperty,
    ) -> Self {
        Self {
            ctx,
            state: OptimizeExprState::Init,
            required,
            group_index,
            m_expr_index,
            children_props: vec![],
            ref_count: Rc::new(SharedCounter::new()),
            parent: None,
        }
    }

    pub fn with_parent(
        ctx: Arc<dyn TableContext>,
        group_index: IndexType,
        m_expr_index: IndexType,
        required: RequiredProperty,
        parent: &Rc<SharedCounter>,
    ) -> Self {
        let mut task = Self::new(ctx, group_index, m_expr_index, required);
        parent.inc();
        task.parent = Some(parent.clone());
        task
    }

    pub fn execute(
        mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        if matches!(self.state, OptimizeExprState::OptimizedSelf) {
            return Ok(());
        }
        self.transition(optimizer, scheduler)?;
        scheduler.add_task(Task::OptimizeExpr(self));
        Ok(())
    }

    fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        let event = match self.state {
            OptimizeExprState::Init => self.optimize_children(optimizer, scheduler)?,
            OptimizeExprState::OptimizedChildren => self.optimize_self(optimizer, scheduler)?,
            OptimizeExprState::OptimizedSelf => unreachable!(),
        };

        match (self.state, event) {
            (OptimizeExprState::Init, OptimizeExprEvent::OptimizingChildren) => {}
            (OptimizeExprState::Init, OptimizeExprEvent::OptimizedChildren) => {
                self.state = OptimizeExprState::OptimizedChildren;
            }
            (OptimizeExprState::OptimizedChildren, OptimizeExprEvent::OptimizingSelf) => {}
            (OptimizeExprState::OptimizedChildren, OptimizeExprEvent::OptimizedSelf) => {
                self.state = OptimizeExprState::OptimizedSelf;
            }
            _ => unreachable!(),
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

        let rel_expr = RelExpr::with_m_expr(m_expr, &optimizer.memo);

        let mut all_children_optimized = true;
        let mut children_props = Vec::with_capacity(m_expr.children.len());
        for (child_index, child) in m_expr.children.iter().enumerate() {
            let group = optimizer.memo.group(*child)?;
            if !group.state.optimized() {
                all_children_optimized = false;
                let child_required = rel_expr.compute_required_prop_child(
                    self.ctx.clone(),
                    child_index,
                    &self.required,
                )?;
                children_props.push(child_required.clone());
                let task = OptimizeGroupTask::with_parent(
                    self.ctx.clone(),
                    *child,
                    child_required,
                    &self.ref_count,
                );
                scheduler.add_task(Task::OptimizeGroup(task));
            }
        }
        if children_props.len() == m_expr.children.len() {
            self.children_props.push(children_props);
        }

        if all_children_optimized {
            Ok(OptimizeExprEvent::OptimizedChildren)
        } else {
            Ok(OptimizeExprEvent::OptimizingChildren)
        }
    }

    fn optimize_self(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        _scheduler: &mut Scheduler,
    ) -> Result<OptimizeExprEvent> {
        for children_prop in self.children_props.iter() {
            let m_expr = optimizer
                .memo
                .group(self.group_index)?
                .m_expr(self.m_expr_index)?
                .clone();

            // Enforce properties if necessary
            let children_physical_props = children_prop
                .iter()
                .map(|prop| PhysicalProperty {
                    distribution: prop.distribution.clone(),
                })
                .collect::<Vec<_>>();
            let prop = m_expr
                .plan
                .derive_physical_prop_with_children_prop(&children_physical_props)?;

            // TODO(leiysky): We only support enforcing distribution property for now, so we can disable
            // enforcer by checking if the query is running in distributed mode. But in the future, we
            // should use a more general way to check if the enforcer is needed.
            if optimizer.enable_distributed_optimization && !self.required.satisfied_by(&prop) {
                // Should add enforcers to enforce the required property
                let enforcers = self.required.distribution.get_enforcers()?;
                for enforcer in enforcers {
                    // Create a `SExpr` for enforcer, the child of which is the current group
                    let enforcer_expr = SExpr::create_unary(
                        enforcer,
                        SExpr::create(
                            PatternPlan {
                                plan_type: RelOp::Pattern,
                            }
                            .into(),
                            vec![],
                            Some(m_expr.group_index),
                            None,
                            None,
                        ),
                    );

                    optimizer.insert_expression(m_expr.group_index, &enforcer_expr)?;
                }

                if let Some(parent) = &self.parent {
                    parent.dec();
                }
                return Ok(OptimizeExprEvent::OptimizedSelf);
            }

            // Cost current expr if no need to add enforcers, in which case the required
            // property is satisfied by the current expr.
            let mut cost = Cost::from(0);

            for (child_group, child_prop) in m_expr.children.iter().zip(children_prop.iter()) {
                let cost_context = optimizer
                    .best_cost_map
                    .get(&(*child_group, child_prop.clone()))
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Cannot find CostContext of group: {child_group}"
                        ))
                    })?;

                cost = cost + cost_context.cost;
            }

            let op_cost = optimizer
                .cost_model
                .compute_cost(&optimizer.memo, &m_expr)?;
            cost = cost + op_cost;

            let cost_context = CostContext {
                cost,
                group_index: m_expr.group_index,
                expr_index: m_expr.index,
                required_prop: self.required.clone(),
                children_prop: children_prop.clone(),
            };

            match optimizer
                .best_cost_map
                .entry((m_expr.group_index, self.required.clone()))
            {
                Entry::Vacant(entry) => {
                    entry.insert(cost_context);
                }
                Entry::Occupied(mut entry) => {
                    // Replace the cost context of the group if current context is lower
                    if cost < entry.get().cost {
                        entry.insert(cost_context);
                    }
                }
            }
        }

        if let Some(parent) = &self.parent {
            parent.dec();
        }

        Ok(OptimizeExprEvent::OptimizedSelf)
    }
}
