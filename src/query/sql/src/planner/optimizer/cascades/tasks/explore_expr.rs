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

use super::apply_rule::ApplyRuleTask;
use super::explore_group::ExploreGroupTask;
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::IndexType;

#[derive(Clone, Copy, Debug)]
pub enum ExploreExprState {
    Init,
    ExploredChildren,
    ExploredSelf,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub enum ExploreExprEvent {
    ExploringChildren,
    ExploredChildren,
    ExploringSelf,
    ExploredSelf,
}

#[derive(Debug)]
pub struct ExploreExprTask {
    pub state: ExploreExprState,

    pub group_index: IndexType,
    pub m_expr_index: IndexType,

    pub ref_count: Rc<SharedCounter>,
    pub parent: Option<Rc<SharedCounter>>,
}

impl ExploreExprTask {
    pub fn new(group_index: IndexType, m_expr_index: IndexType) -> Self {
        Self {
            state: ExploreExprState::Init,
            group_index,
            m_expr_index,
            ref_count: Rc::new(SharedCounter::new()),
            parent: None,
        }
    }

    pub fn with_parent(
        group_index: IndexType,
        m_expr_index: IndexType,
        parent: &Rc<SharedCounter>,
    ) -> Self {
        let mut task = Self::new(group_index, m_expr_index);
        parent.inc();
        task.parent = Some(parent.clone());
        task
    }

    pub fn execute(
        mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        if matches!(self.state, ExploreExprState::ExploredSelf) {
            return Ok(());
        }
        self.transition(optimizer, scheduler)?;
        scheduler.add_task(Task::ExploreExpr(self));
        Ok(())
    }

    fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        let event = match self.state {
            ExploreExprState::Init => self.explore_children(optimizer, scheduler)?,
            ExploreExprState::ExploredChildren => self.explore_self(optimizer, scheduler)?,
            ExploreExprState::ExploredSelf => ExploreExprEvent::ExploredSelf,
        };

        // Transition the state machine with event
        match (self.state, event) {
            (ExploreExprState::Init, ExploreExprEvent::ExploringChildren) => {}
            (ExploreExprState::Init, ExploreExprEvent::ExploredChildren) => {
                self.state = ExploreExprState::ExploredChildren;
            }
            (ExploreExprState::ExploredChildren, ExploreExprEvent::ExploringSelf) => {}
            (ExploreExprState::ExploredChildren, ExploreExprEvent::ExploredSelf) => {
                self.state = ExploreExprState::ExploredSelf;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn explore_children(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<ExploreExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;
        let mut all_children_explored = true;
        for child in m_expr.children.iter() {
            let group = optimizer.memo.group(*child)?;
            if !group.state.explored() {
                // If the child group isn't explored, then schedule a `ExploreGroupTask` for it.
                all_children_explored = false;
                let explore_group_task = ExploreGroupTask::with_parent(*child, &self.ref_count);
                scheduler.add_task(Task::ExploreGroup(explore_group_task));
            }
        }

        if all_children_explored {
            Ok(ExploreExprEvent::ExploredChildren)
        } else {
            Ok(ExploreExprEvent::ExploringChildren)
        }
    }

    fn explore_self(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<ExploreExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;

        for rule in optimizer.explore_rules.iter() {
            let apply_rule_task = ApplyRuleTask::with_parent(
                rule.id(),
                m_expr.group_index,
                m_expr.index,
                &self.ref_count,
            );
            scheduler.add_task(Task::ApplyRule(apply_rule_task));
        }

        if let Some(parent) = &self.parent {
            parent.dec();
        }
        Ok(ExploreExprEvent::ExploredSelf)
    }
}
