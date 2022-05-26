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
// limitations under the License.#[derive(Clone, Debug)]

use common_exception::Result;

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::PlanType;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::IndexType;

/// Evaluate scalar expression
#[derive(Clone, Debug)]
pub struct EvalScalar {
    pub items: Vec<ScalarItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScalarItem {
    pub scalar: Scalar,
    pub index: IndexType,
}

impl Operator for EvalScalar {
    fn plan_type(&self) -> PlanType {
        PlanType::EvalScalar
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        todo!()
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        Some(self)
    }
}

impl PhysicalPlan for EvalScalar {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        todo!()
    }
}

impl LogicalPlan for EvalScalar {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns;
        for item in self.items.iter() {
            output_columns.insert(item.index);
        }

        // Derive outer columns
        let mut outer_columns = input_prop.outer_columns;
        for item in self.items.iter() {
            let used_columns = item.scalar.used_columns();
            let outer = used_columns
                .difference(&output_columns)
                .cloned()
                .collect::<ColumnSet>();
            outer_columns = outer_columns.union(&outer).cloned().collect();
        }
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
        })
    }
}
