// Copyright 2021 Datafuse Labs
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

use std::hash::Hash;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::ColumnSet;
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MaterializedCTE {
    pub cte_name: String,
    pub required: ColumnSet,
}

impl MaterializedCTE {
    pub fn new(cte_name: String, required: ColumnSet) -> Self {
        Self { cte_name, required }
    }
}

impl Operator for MaterializedCTE {
    fn rel_op(&self) -> RelOp {
        RelOp::MaterializedCTE
    }

    /// Get arity of this operator
    fn arity(&self) -> usize {
        2
    }

    /// Derive relational property
    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        rel_expr.derive_relational_prop_child(1)
    }

    /// Derive physical property
    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(1)
    }

    /// Derive statistics information
    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        rel_expr.derive_cardinality_child(1)
    }
}
