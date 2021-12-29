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

#[derive(Clone, Default, PartialEq, Debug)]
pub struct LogicalGet {
    pub table_index: IndexType,
    pub columns: ColumnSet,
}

impl LogicalGet {
    pub fn from_plan(plan: Plan) -> Result<Self> {
        match plan {
            Plan::LogicalGet(result) => Ok(result),
            _ => Err(ErrorCode::LogicalError("Invalid downcast")),
        }
    }
}

impl LogicalPlan for LogicalGet {
    fn compute_relational_prop(&self, _expression: &SExpr) -> RelationalProperty {
        let output_columns: ColumnSet = self.columns.iter().cloned().collect();
        RelationalProperty::create(output_columns)
    }

    fn as_plan(&self) -> Plan {
        Plan::LogicalGet(self.clone())
    }
}
