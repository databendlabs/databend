// Copyright 2022 Datafuse Labs.
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

use std::rc::Rc;

use common_exception::Result;

use super::implement_group::ImplementGroupTask;
use super::optimize_expr::OptimizeExprTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::group::GroupState;
use crate::plans::Operator;
use crate::IndexType;

#[derive(Clone, Copy, Debug)]
pub enum OptimizeGroupState {
    Init,
    Implemented,
    Optimized,
}

#[derive(Clone, Copy, Debug)]
pub enum OptimizeGroupEvent {
    Implementing,
    Implemented,
    Optimizing,
    Optimized,
}

#[derive(Debug)]
pub struct OptimizeGroupTask {
    pub state: OptimizeGroupState,
    pub group_index: IndexType,
    pub last_optimized_expr_index: Option<IndexType>,

    pub ref_count: Rc<SharedCounter>,
    pub parent: Option<Rc<SharedCounter>>,
}

impl OptimizeGroupTask {
    pub fn new(group_index: IndexType) -> Self {
        Self {
            state: OptimizeGroupState::Init,
            group_index,
            last_optimized_expr_index: None,
            ref_count: Rc::new(SharedCounter::new()),
            parent: None,
        }
    }

    pub fn with_parent(group_index: IndexType, parent: &Rc<SharedCounter>) -> Self {
        let mut task = Self::new(group_index);
        parent.inc();
        task.parent = Some(parent.clone());
        task
    }

    pub fn execute(
        mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        if matches!(self.state, OptimizeGroupState::Optimized) {
            return Ok(());
        }
        self.transition(optimizer, scheduler)?;
        scheduler.add_task(Task::OptimizeGroup(self));
        Ok(())
    }

    fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        let event = match self.state {
            OptimizeGroupState::Init => self.implement_group(optimizer, scheduler)?,
            OptimizeGroupState::Implemented => self.optimize_group(optimizer, scheduler)?,
            OptimizeGroupState::Optimized => unreachable!(),
        };

        match (self.state, event) {
            (OptimizeGroupState::Init, OptimizeGroupEvent::Implementing) => {}
            (OptimizeGroupState::Init, OptimizeGroupEvent::Implemented) => {
                self.state = OptimizeGroupState::Implemented;
            }
            (OptimizeGroupState::Implemented, OptimizeGroupEvent::Optimizing) => {}
            (OptimizeGroupState::Implemented, OptimizeGroupEvent::Optimized) => {
                self.state = OptimizeGroupState::Optimized;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn implement_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<OptimizeGroupEvent> {
        let group = optimizer.memo.group(self.group_index)?;
        if !group.state.implemented() {
            let task = ImplementGroupTask::with_parent(group.group_index, &self.ref_count);
            scheduler.add_task(Task::ImplementGroup(task));
            Ok(OptimizeGroupEvent::Implementing)
        } else {
            Ok(OptimizeGroupEvent::Implemented)
        }
    }

    fn optimize_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<OptimizeGroupEvent> {
        let group = optimizer.memo.group_mut(self.group_index)?;

        // Check if there is new added `MExpr`s.
        let start_index = self.last_optimized_expr_index.unwrap_or_default();
        if start_index == group.num_exprs() {
            group.set_state(GroupState::Optimized);
            if let Some(parent) = &self.parent {
                parent.dec();
            }
            return Ok(OptimizeGroupEvent::Optimized);
        }

        for m_expr in group.m_exprs.iter() {
            if m_expr.plan.is_physical() {
                let task =
                    OptimizeExprTask::with_parent(self.group_index, m_expr.index, &self.ref_count);
                scheduler.add_task(Task::OptimizeExpr(task));
            }
        }

        self.last_optimized_expr_index = Some(group.num_exprs());

        Ok(OptimizeGroupEvent::Optimizing)
    }
}
