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

#[derive(Educe)]
#[educe(Debug)]
pub struct ExploreExprTask {
    #[educe(Debug(ignore))]
    pub ctx: Arc<dyn TableContext>,

    pub state: ExploreExprState,

    pub group_index: IndexType,
    pub m_expr_index: IndexType,

    pub ref_count: SharedCounter,
    pub parent: Option<SharedCounter>,
}

impl ExploreExprTask {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        group_index: IndexType,
        m_expr_index: IndexType,
    ) -> Self {
        Self {
            ctx,
            state: ExploreExprState::Init,
            group_index,
            m_expr_index,
            ref_count: SharedCounter::new(),
            parent: None,
        }
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
        if matches!(self.state, ExploreExprState::ExploredSelf) {
            return Ok(None);
        }
        self.transition(optimizer, scheduler)?;
        Ok(Some(Task::ExploreExpr(self)))
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
            _ => Err(ErrorCode::Internal(format!(
                "Invalid transition from {:?} with {:?}",
                self.state, event
            )))?,
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
                let explore_group_task = ExploreGroupTask::new(self.ctx.clone(), *child)
                    .with_parent(self.ref_count.clone());
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
        let rule_set = &optimizer.explore_rule_set;

        for rule_id in rule_set.iter() {
            let apply_rule_task =
                ApplyRuleTask::new(rule_id, m_expr.group_index, m_expr.index)
                    .with_parent(self.ref_count.clone());
            scheduler.add_task(Task::ApplyRule(apply_rule_task));
        }

        Ok(ExploreExprEvent::ExploredSelf)
    }
}
