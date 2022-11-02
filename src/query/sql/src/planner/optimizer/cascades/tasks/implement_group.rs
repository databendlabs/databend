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

use common_exception::ErrorCode;
use common_exception::Result;

use super::explore_group::ExploreGroupTask;
use super::implement_expr::ImplementExprTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::group::GroupState;
use crate::IndexType;

#[derive(Clone, Copy, Debug)]
pub enum ImplementGroupState {
    Init,
    Explored,
    Implemented,
}

#[derive(Clone, Copy, Debug)]
pub enum ImplementGroupEvent {
    Exploring,
    Explored,
    Implementing,
    Implemented,
}

#[derive(Debug)]
pub struct ImplementGroupTask {
    pub state: ImplementGroupState,

    pub group_index: IndexType,
    pub last_implemented_expr_index: Option<IndexType>,

    pub ref_count: Rc<SharedCounter>,
    pub parent: Option<Rc<SharedCounter>>,
}

impl ImplementGroupTask {
    pub fn new(group_index: IndexType) -> Self {
        Self {
            state: ImplementGroupState::Init,
            group_index,
            last_implemented_expr_index: None,
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
        if matches!(self.state, ImplementGroupState::Implemented) {
            return Ok(());
        }
        self.transition(optimizer, scheduler)?;
        scheduler.add_task(super::Task::ImplementGroup(self));
        Ok(())
    }

    pub fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        let event = match self.state {
            ImplementGroupState::Init => self.explore_group(optimizer, scheduler)?,
            ImplementGroupState::Explored => self.implement_group(optimizer, scheduler)?,
            ImplementGroupState::Implemented => unreachable!(),
        };

        // Transition the state machine with event
        match (self.state, event) {
            (ImplementGroupState::Init, ImplementGroupEvent::Exploring) => {}
            (ImplementGroupState::Init, ImplementGroupEvent::Explored) => {
                self.state = ImplementGroupState::Explored;
            }
            (ImplementGroupState::Explored, ImplementGroupEvent::Implementing) => {}
            (ImplementGroupState::Explored, ImplementGroupEvent::Implemented) => {
                self.state = ImplementGroupState::Implemented
            }
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "Cannot transition state {:?} with event {:?}",
                    &self.state, &event
                )));
            }
        }

        Ok(())
    }

    fn explore_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<ImplementGroupEvent> {
        let group = optimizer.memo.group(self.group_index)?;
        if !group.state.explored() {
            let explore_group_task =
                ExploreGroupTask::with_parent(self.group_index, &self.ref_count);
            scheduler.add_task(Task::ExploreGroup(explore_group_task));
            Ok(ImplementGroupEvent::Exploring)
        } else {
            Ok(ImplementGroupEvent::Explored)
        }
    }

    fn implement_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<ImplementGroupEvent> {
        let group = optimizer.memo.group_mut(self.group_index)?;

        // Check if there is new added `MExpr`s.
        let start_index = self.last_implemented_expr_index.unwrap_or_default();
        if start_index == group.num_exprs() {
            group.set_state(GroupState::Implemented);
            if let Some(parent) = &self.parent {
                parent.dec();
            }
            return Ok(ImplementGroupEvent::Implemented);
        }

        for m_expr in group.m_exprs.iter().skip(start_index) {
            let task =
                ImplementExprTask::with_parent(m_expr.group_index, m_expr.index, &self.ref_count);
            scheduler.add_task(Task::ImplementExpr(task));
        }

        self.last_implemented_expr_index = Some(group.num_exprs());

        Ok(ImplementGroupEvent::Implementing)
    }
}
