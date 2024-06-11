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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use crate::optimizer::ColumnSet;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::IndexType;

/// Evaluate scalar expression
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EvalScalar {
    pub items: Vec<ScalarItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScalarItem {
    pub scalar: ScalarExpr,
    // The index of the derived column in metadata
    pub index: IndexType,
}

impl EvalScalar {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for item in self.items.iter() {
            used_columns.insert(item.index);
            used_columns.extend(item.scalar.used_columns());
        }
        Ok(used_columns)
    }
}

impl Operator for EvalScalar {
    fn rel_op(&self) -> RelOp {
        RelOp::EvalScalar
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Ok(required.clone())
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        for item in self.items.iter() {
            output_columns.insert(item.index);
        }

        // Derive outer columns
        let mut outer_columns = input_prop
            .outer_columns
            .difference(&input_prop.output_columns)
            .cloned()
            .collect::<ColumnSet>();
        for item in self.items.iter() {
            let used_columns = item.scalar.used_columns();
            let outer = used_columns.difference(&input_prop.output_columns).cloned();
            outer_columns.extend(outer);
        }

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.clone());

        // Derive orderings
        let orderings = input_prop.orderings.clone();
        let partition_orderings = input_prop.partition_orderings.clone();

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        rel_expr.derive_cardinality_child(0)
    }
}
