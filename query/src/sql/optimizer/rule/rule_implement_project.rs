// Copyright 2021 Datafuse Labs.
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

use crate::sql::optimizer::rule::transform_state::TransformState;
use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::LogicalProject;
use crate::sql::PhysicalProject;
use crate::sql::Plan;

pub struct RuleImplementProject {
    id: RuleID,
    pattern: SExpr,
}

impl RuleImplementProject {
    pub fn create() -> Self {
        RuleImplementProject {
            id: RuleID::ImplementProject,
            pattern: SExpr::create_unary(
                Rc::new(Plan::LogicalProject(LogicalProject::default())),
                SExpr::create_leaf(Rc::new(Plan::Pattern)),
            ),
        }
    }
}

impl Rule for RuleImplementProject {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, expression: &SExpr, state: &mut TransformState) -> Result<()> {
        let project = LogicalProject::from_plan((*expression.plan()).clone())?;
        let physical = PhysicalProject::create(project.items);
        let result = SExpr::create(
            Rc::new(Plan::PhysicalProject(physical)),
            expression.children().clone(),
            None,
        );
        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
