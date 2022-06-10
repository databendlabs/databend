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

use common_exception::Result;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::Project;
use crate::sql::plans::RelOp;

pub struct RuleEliminateProject {
    id: RuleID,
    pattern: SExpr,
}

impl RuleEliminateProject {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateProject,
            // Project
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Project,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
        }
    }
}

impl Rule for RuleEliminateProject {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let eval_scalar: Project = s_expr.plan().clone().try_into()?;
        if eval_scalar.columns.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
