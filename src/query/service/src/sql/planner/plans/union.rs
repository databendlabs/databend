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

use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;

#[derive(Clone, Debug)]
pub struct Union;

impl Operator for Union {
    fn rel_op(&self) -> RelOp {
        RelOp::Union
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

impl PhysicalOperator for Union {
    fn derive_physical_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        todo!()
    }

    fn compute_required_prop_child<'a>(
        &self,
        _rel_expr: &RelExpr<'a>,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        todo!()
    }
}

impl LogicalOperator for Union {
    fn derive_relational_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        todo!()
    }
}
