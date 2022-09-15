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

use crate::sql::optimizer::rule::transform_state::TransformState;
use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::RelOp;

pub struct RuleImplementGet {
    id: RuleID,
    pattern: SExpr,
}

impl RuleImplementGet {
    pub fn new() -> Self {
        RuleImplementGet {
            id: RuleID::ImplementGet,
            pattern: SExpr::create_leaf(
                PatternPlan {
                    plan_type: RelOp::LogicalGet,
                }
                .into(),
            ),
        }
    }
}

impl Rule for RuleImplementGet {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let plan = s_expr.plan().clone();
        let logical_get: LogicalGet = plan.try_into()?;

        let result = SExpr::create_leaf(
            PhysicalScan {
                table_index: logical_get.table_index,
                columns: logical_get.columns,
                push_down_predicates: logical_get.push_down_predicates,
                limit: logical_get.limit,
                order_by: logical_get.order_by,
                prewhere: logical_get.prewhere,
            }
            .into(),
        );
        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
