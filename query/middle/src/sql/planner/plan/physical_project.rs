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

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::plan::logical_project::ProjectItem;
use crate::sql::PhysicalPlan;
use crate::sql::Plan;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct PhysicalProject {
    pub items: Vec<ProjectItem>,
}

impl PhysicalProject {
    pub fn create(items: Vec<ProjectItem>) -> Self {
        PhysicalProject { items }
    }
}

impl PhysicalPlan for PhysicalProject {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        PhysicalProperty::default()
    }

    fn compute_required_prop(&self, input_prop: &RequiredProperty) -> RequiredProperty {
        let mut required_columns: ColumnSet = input_prop.required_columns().clone();
        for item in self.items.iter() {
            let used_columns = item.expr.used_columns();
            required_columns = required_columns.union(&used_columns).cloned().collect();
        }
        RequiredProperty::create(required_columns)
    }

    fn as_plan(&self) -> Plan {
        Plan::PhysicalProject(self.clone())
    }
}
