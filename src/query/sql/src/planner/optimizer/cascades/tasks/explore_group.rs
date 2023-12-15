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

use std::rc::Rc;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use educe::Educe;

use super::explore_expr::ExploreExprTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::group::GroupState;
use crate::IndexType;

#[derive(Clone, Copy, Debug)]
pub enum ExploreGroupState {
    Init,
    Explored,
}

#[derive(Clone, Copy, Debug)]
pub enum ExploreGroupEvent {
    Exploring,
    Explored,
}

#[derive(Educe)]
#[educe(Debug)]
pub struct ExploreGroupTask {
    #[educe(Debug(ignore))]
    pub ctx: Arc<dyn TableContext>,

    pub state: ExploreGroupState,

    pub group_index: IndexType,
    pub last_explored_m_expr: Option<IndexType>,

    pub ref_count: Rc<SharedCounter>,
    pub parent: Option<Rc<SharedCounter>>,

    pub should_terminate: bool,
}

impl ExploreGroupTask {
    pub fn new(ctx: Arc<dyn TableContext>, group_index: IndexType) -> Self {
        Self {
            ctx,
            state: ExploreGroupState::Init,
            group_index,
            last_explored_m_expr: None,
            ref_count: Rc::new(SharedCounter::new()),
            parent: None,
            should_terminate: false,
        }
    }

    pub fn with_parent(mut self, parent: &Rc<SharedCounter>) -> Self {
        parent.inc();
        self.parent = Some(parent.clone());
        self
    }

    /// Mark this task as a termination task.
    pub fn with_termination(mut self) -> Self {
        self.should_terminate = true;
        self
    }

    pub fn execute(
        mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler<'_>,
    ) -> Result<()> {
        if matches!(self.state, ExploreGroupState::Explored) {
            return Ok(());
        }
        if self.should_terminate {
            return self.terminate(optimizer);
        }
        self.transition(optimizer, scheduler)?;
        scheduler.add_task(Task::ExploreGroup(self));
        Ok(())
    }

    pub fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler<'_>,
    ) -> Result<()> {
        let event = match self.state {
            ExploreGroupState::Init => self.explore_group(optimizer, scheduler)?,
            ExploreGroupState::Explored => unreachable!(),
        };

        // Transition the state machine with event
        match (self.state, event) {
            (ExploreGroupState::Init, ExploreGroupEvent::Exploring) => {}
            (ExploreGroupState::Init, ExploreGroupEvent::Explored) => {
                self.state = ExploreGroupState::Explored;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn explore_group(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler<'_>,
    ) -> Result<ExploreGroupEvent> {
        let group = optimizer.memo.group_mut(self.group_index)?;

        // Check if there is new added `MExpr`s.
        let start_index = self.last_explored_m_expr.unwrap_or_default();
        if start_index == group.num_exprs() {
            group.set_state(GroupState::Explored);
            if let Some(parent) = &self.parent {
                parent.dec();
            }
            return Ok(ExploreGroupEvent::Explored);
        }

        for m_expr in group.m_exprs.iter().skip(start_index) {
            let task = ExploreExprTask::new(self.ctx.clone(), m_expr.group_index, m_expr.index)
                .with_parent(&self.ref_count);
            scheduler.add_task(Task::ExploreExpr(task));
        }

        self.last_explored_m_expr = Some(group.num_exprs());

        Ok(ExploreGroupEvent::Exploring)
    }

    pub fn terminate(&mut self, cascades_optimizer: &mut CascadesOptimizer) -> Result<()> {
        if let Some(parent) = &self.parent {
            parent.dec();
        }

        let group = cascades_optimizer.memo.group_mut(self.group_index)?;
        group.set_state(GroupState::Explored);

        Ok(())
    }
}
