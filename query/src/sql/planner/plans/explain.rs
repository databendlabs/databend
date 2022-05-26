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

use common_ast::ast::ExplainKind;
use common_exception::Result;

use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::PlanType;

#[derive(Clone, Debug)]
pub struct ExplainPlan {
    pub explain_kind: ExplainKind,
}

impl Operator for ExplainPlan {
    fn plan_type(&self) -> PlanType {
        PlanType::Explain
    }

    fn is_physical(&self) -> bool {
        false
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        Some(self)
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        None
    }
}

impl LogicalPlan for ExplainPlan {
    fn derive_relational_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        Ok(RelationalProperty::default())
    }
}
