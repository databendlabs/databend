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
use crate::sql::IndexType;
use crate::sql::PhysicalPlan;
use crate::sql::Plan;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct PhysicalScan {
    pub table_index: IndexType,
    pub columns: ColumnSet,
}

impl PhysicalScan {
    pub fn create(table_index: IndexType, columns: ColumnSet) -> Self {
        PhysicalScan {
            table_index,
            columns,
        }
    }
}

impl PhysicalPlan for PhysicalScan {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        PhysicalProperty::default()
    }

    fn compute_required_prop(&self, input_prop: &RequiredProperty) -> RequiredProperty {
        input_prop.clone()
    }

    fn as_plan(&self) -> Plan {
        Plan::PhysicalScan(self.clone())
    }
}
