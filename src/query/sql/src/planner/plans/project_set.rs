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

use std::ops::Deref;
use std::sync::Arc;

use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::IndexType;
use crate::ScalarExpr;

/// An item of set-returning function.
/// Contains definition of srf and its output columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SrfItem {
    pub scalar: ScalarExpr,
    pub index: IndexType,
}

/// `ProjectSet` is a plan that evaluate a series of
/// set-returning functions, zip the result together,
/// and return the joined result with input relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectSet {
    pub srfs: Vec<SrfItem>,
}

impl Operator for ProjectSet {
    fn rel_op(&self) -> RelOp {
        RelOp::ProjectSet
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<RelationalProperty>> {
        let child_prop = rel_expr.derive_relational_prop_child(0)?.as_ref().clone();

        // Derive output columns
        let mut output_columns = child_prop.output_columns.clone();
        for srf in &self.srfs {
            output_columns.insert(srf.index);
        }

        // Derive used columns
        let mut used_columns = child_prop.used_columns.clone();
        for srf in &self.srfs {
            used_columns.extend(srf.scalar.used_columns());
        }

        // Derive outer columns
        let mut outer_columns = child_prop.outer_columns.clone();
        for srf in &self.srfs {
            outer_columns.extend(
                srf.scalar
                    .used_columns()
                    .difference(&child_prop.output_columns)
                    .cloned(),
            );
        }

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> databend_common_exception::Result<Arc<StatInfo>> {
        let mut input_stat = rel_expr.derive_cardinality_child(0)?.deref().clone();
        // ProjectSet is set-returning functions, precise_cardinality set None
        input_stat.statistics.precise_cardinality = None;
        Ok(Arc::new(input_stat))
    }
}
