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

use super::optimize_expr::OptimizeExprTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::ExploreGroupTask;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::RelExpr;
use crate::optimizer::RequiredProperty;
use crate::plans::Operator;
use crate::IndexType;

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
        scheduler: &mut Scheduler,
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
        scheduler: &mut Scheduler,
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
        scheduler: &mut Scheduler,
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
        scheduler: &mut Scheduler,
    ) -> Result<OptimizeGroupEvent> {
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
            if Some((self.group_index, m_expr.index)) == self.owner_expr {
                continue;
            }

            let rel_expr = RelExpr::with_m_expr(m_expr, &optimizer.memo);
            let children_required_props = if optimizer.enforce_distribution {
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
