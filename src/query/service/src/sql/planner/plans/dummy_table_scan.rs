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

use super::LogicalOperator;
use super::Operator;
use super::PhysicalOperator;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelationalProperty;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DummyTableScan;

impl Operator for DummyTableScan {
    fn rel_op(&self) -> super::RelOp {
        super::RelOp::DummyTableScan
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        Some(self)
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        Some(self)
    }
}

impl LogicalOperator for DummyTableScan {
    fn derive_relational_prop<'a>(
        &self,
        _rel_expr: &crate::sql::optimizer::RelExpr<'a>,
    ) -> common_exception::Result<crate::sql::optimizer::RelationalProperty> {
        Ok(RelationalProperty {
            output_columns: ColumnSet::new(),
            outer_columns: ColumnSet::new(),
            cardinality: 1.0,
            precise_cardinality: Some(1),
        })
    }
}

impl PhysicalOperator for DummyTableScan {
    fn derive_physical_prop<'a>(
        &self,
        _rel_expr: &crate::sql::optimizer::RelExpr<'a>,
    ) -> common_exception::Result<crate::sql::optimizer::PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: crate::sql::optimizer::Distribution::Serial,
        })
    }

    fn compute_required_prop_child<'a>(
        &self,
        _rel_expr: &crate::sql::optimizer::RelExpr<'a>,
        _child_index: usize,
        required: &crate::sql::optimizer::RequiredProperty,
    ) -> common_exception::Result<crate::sql::optimizer::RequiredProperty> {
        Ok(required.clone())
    }
}
