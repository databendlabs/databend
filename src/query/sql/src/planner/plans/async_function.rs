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
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;

/// `AsyncFunction` is a plan that evaluate a series of async functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AsyncFunction {
    pub items: Vec<ScalarItem>,
}

impl AsyncFunction {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for item in self.items.iter() {
            used_columns.insert(item.index);
            used_columns.extend(item.scalar.used_columns());
        }
        Ok(used_columns)
    }
}

impl Operator for AsyncFunction {
    fn rel_op(&self) -> RelOp {
        RelOp::AsyncFunction
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        for item in self.items.iter() {
            output_columns.insert(item.index);
        }

        // Derive outer columns
        let mut outer_columns = input_prop.outer_columns.clone();
        for item in self.items.iter() {
            let used_columns = item.scalar.used_columns();
            let outer = used_columns
                .difference(&output_columns)
                .cloned()
                .collect::<ColumnSet>();
            outer_columns = outer_columns.union(&outer).cloned().collect();
        }
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

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

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        Ok(vec![vec![required.clone()]])
    }
}
