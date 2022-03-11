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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::IndexType;
use crate::sql::LogicalPlan;
use crate::sql::Plan;
use crate::sql::ScalarExpr;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct LogicalProject {
    pub items: Vec<ProjectItem>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct ProjectItem {
    pub index: IndexType,
    pub expr: ScalarExpr,
}

impl LogicalProject {
    pub fn create(items: Vec<ProjectItem>) -> Self {
        LogicalProject { items }
    }

    pub fn from_plan(plan: Plan) -> Result<Self> {
        match plan {
            Plan::LogicalProject(project) => Ok(project),
            _ => Err(ErrorCode::LogicalError("Invalid downcast")),
        }
    }
}

impl LogicalPlan for LogicalProject {
    fn compute_relational_prop(&self, _expression: &SExpr) -> RelationalProperty {
        let output_columns: ColumnSet = self.items.iter().map(|item| item.index).collect();
        RelationalProperty::create(output_columns)
    }

    fn as_plan(&self) -> Plan {
        Plan::LogicalProject(self.clone())
    }
}
