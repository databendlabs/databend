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
use crate::sql::optimizer::RequiredProperty;
use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    Hash(Vec<Scalar>),
    Broadcast,
    Merge,
}

impl Operator for Exchange {
    fn rel_op(&self) -> RelOp {
        RelOp::Exchange
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        false
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        Some(self)
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        None
    }
}

impl PhysicalOperator for Exchange {
    fn derive_physical_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: match self {
                Exchange::Hash(hash_keys) => Distribution::Hash(hash_keys.clone()),
                Exchange::Broadcast => Distribution::Broadcast,
                Exchange::Merge => Distribution::Serial,
            },
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
