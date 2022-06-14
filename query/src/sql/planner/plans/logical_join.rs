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

use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::Result;

use super::ScalarExpr;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => {
                write!(f, "INNER")
            }
            JoinType::Left => {
                write!(f, "LEFT OUTER")
            }
            JoinType::Right => {
                write!(f, "RIGHT OUTER")
            }
            JoinType::Full => {
                write!(f, "FULL OUTER")
            }
            JoinType::Semi => {
                write!(f, "SEMI")
            }
            JoinType::Anti => {
                write!(f, "ANTI")
            }
            JoinType::Cross => {
                write!(f, "CROSS")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogicalInnerJoin {
    pub left_conditions: Vec<Scalar>,
    pub right_conditions: Vec<Scalar>,
    pub join_type: JoinType,
}

impl Operator for LogicalInnerJoin {
    fn plan_type(&self) -> RelOp {
        RelOp::LogicalInnerJoin
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

impl LogicalPlan for LogicalInnerJoin {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        // Derive output columns
        let mut output_columns = left_prop.output_columns;
        output_columns = output_columns
            .union(&right_prop.output_columns)
            .cloned()
            .collect();

        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns;
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();
        for cond in self
            .left_conditions
            .iter()
            .chain(self.right_conditions.iter())
        {
            let used_columns = cond.used_columns();
            let outer = used_columns.difference(&output_columns).cloned().collect();
            outer_columns = outer_columns.union(&outer).cloned().collect();
        }
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
        })
    }
}
