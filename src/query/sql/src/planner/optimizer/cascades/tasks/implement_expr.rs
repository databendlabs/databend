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
use super::Task;
use crate::optimizer::cascades::scheduler::Scheduler;
use crate::optimizer::cascades::tasks::ImplementGroupTask;
use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::IndexType;

#[derive(Clone, Copy, Debug)]
pub enum ImplementExprState {
    Init,
    ImplementedChildren,
    ImplementedSelf,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub enum ImplementExprEvent {
    ImplementingChildren,
    ImplementedChildren,
    ImplementingSelf,
    ImplementedSelf,
}

#[derive(Debug)]
pub struct ImplementExprTask {
    pub state: ImplementExprState,

    pub group_index: IndexType,
    pub m_expr_index: IndexType,

    pub ref_count: Rc<SharedCounter>,
    pub parent: Option<Rc<SharedCounter>>,
}

impl ImplementExprTask {
    pub fn new(group_index: IndexType, m_expr_index: IndexType) -> Self {
        Self {
            state: ImplementExprState::Init,
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
        if matches!(self.state, ImplementExprState::ImplementedSelf) {
            return Ok(());
        }
        self.transition(optimizer, scheduler)?;
        scheduler.add_task(Task::ImplementExpr(self));
        Ok(())
    }

    fn transition(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        let event = match self.state {
            ImplementExprState::Init => self.implement_children(optimizer, scheduler)?,
            ImplementExprState::ImplementedChildren => self.implement_self(optimizer, scheduler)?,
            ImplementExprState::ImplementedSelf => unreachable!(),
        };

        // Transition the state machine with event
        match (self.state, event) {
            (ImplementExprState::Init, ImplementExprEvent::ImplementingChildren) => {}
            (ImplementExprState::Init, ImplementExprEvent::ImplementedChildren) => {
                self.state = ImplementExprState::ImplementedChildren;
            }
            (ImplementExprState::ImplementedChildren, ImplementExprEvent::ImplementingSelf) => {}
            (ImplementExprState::ImplementedChildren, ImplementExprEvent::ImplementedSelf) => {
                self.state = ImplementExprState::ImplementedSelf;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn implement_children(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<ImplementExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;
        let mut all_children_implemented = true;
        for child in m_expr.children.iter() {
            let group = optimizer.memo.group(*child)?;
            if !group.state.implemented() {
                // If the child group isn't explored, then schedule a `ExploreGroupTask` for it.
                all_children_implemented = false;
                let task = ImplementGroupTask::with_parent(*child, &self.ref_count);
                scheduler.add_task(Task::ImplementGroup(task));
            }
        }

        if all_children_implemented {
            Ok(ImplementExprEvent::ImplementedChildren)
        } else {
            Ok(ImplementExprEvent::ImplementingChildren)
        }
    }

    fn implement_self(
        &mut self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<ImplementExprEvent> {
        let m_expr = optimizer
            .memo
            .group(self.group_index)?
            .m_expr(self.m_expr_index)?;

        for rule in optimizer.implement_rules.iter() {
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

        Ok(ImplementExprEvent::ImplementedSelf)
    }
}
