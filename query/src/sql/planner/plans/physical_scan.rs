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

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::RelOp;
use crate::sql::IndexType;

#[derive(Clone, Debug)]
pub struct PhysicalScan {
    pub table_index: IndexType,
    pub columns: ColumnSet,
}

impl Operator for PhysicalScan {
    fn rel_op(&self) -> RelOp {
        RelOp::PhysicalScan
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        false
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        todo!()
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        None
    }
}

impl PhysicalPlan for PhysicalScan {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        todo!()
    }
}
