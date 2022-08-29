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

use common_exception::Result;

use crate::sql::optimizer::Distribution;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnionAll;

impl Operator for UnionAll {
    fn rel_op(&self) -> RelOp {
        RelOp::UnionAll
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        Some(self)
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        Some(self)
    }
}

impl LogicalOperator for UnionAll {
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

        let cardinality = left_prop.cardinality + right_prop.cardinality;

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            cardinality,
        })
    }
}

impl PhysicalOperator for UnionAll {
    fn derive_physical_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn compute_required_prop_child<'a>(
        &self,
        _rel_expr: &RelExpr<'a>,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Ok(required.clone())
    }
}
