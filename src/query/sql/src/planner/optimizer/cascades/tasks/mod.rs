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

use std::cell::RefCell;

use databend_common_exception::Result;

pub use self::apply_rule::ApplyRuleTask;
pub use self::explore_expr::ExploreExprTask;
pub use self::explore_group::ExploreGroupTask;
pub use self::optimize_expr::OptimizeExprTask;
pub use self::optimize_group::OptimizeGroupTask;
use super::scheduler::Scheduler;
use super::CascadesOptimizer;

mod apply_rule;
mod explore_expr;
mod explore_group;
mod optimize_expr;
mod optimize_group;

#[derive(Debug)]
pub struct SharedCounter {
    count: RefCell<usize>,
}

impl SharedCounter {
    fn new() -> Self {
        Self {
            count: RefCell::new(0),
        }
    }

    fn inc(&self) {
        *self.count.borrow_mut() += 1;
    }

    fn dec(&self) {
        if *self.count.borrow() > 0 {
            *self.count.borrow_mut() -= 1;
        }
    }

    fn get(&self) -> usize {
        *self.count.borrow()
    }
}

#[derive(Debug)]
pub enum Task {
    ApplyRule(ApplyRuleTask),
    OptimizeGroup(OptimizeGroupTask),
    OptimizeExpr(OptimizeExprTask),
    ExploreGroup(ExploreGroupTask),
    ExploreExpr(ExploreExprTask),
}

impl Task {
    pub fn execute(
        self,
        optimizer: &mut CascadesOptimizer,
        scheduler: &mut Scheduler,
    ) -> Result<()> {
        match self {
            Task::ApplyRule(task) => task.execute(optimizer),
            Task::OptimizeGroup(task) => task.execute(optimizer, scheduler),
            Task::OptimizeExpr(task) => task.execute(optimizer, scheduler),
            Task::ExploreGroup(task) => task.execute(optimizer, scheduler),
            Task::ExploreExpr(task) => task.execute(optimizer, scheduler),
        }
    }

    // Reference count of current task.
    pub fn ref_count(&self) -> usize {
        match self {
            Task::ApplyRule(_) => 0,
            Task::OptimizeGroup(task) => task.ref_count.get(),
            Task::OptimizeExpr(task) => task.ref_count.get(),
            Task::ExploreGroup(task) => task.ref_count.get(),
            Task::ExploreExpr(task) => task.ref_count.get(),
        }
    }
}
