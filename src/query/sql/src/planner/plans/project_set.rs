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

use databend_common_exception::Result;

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;

/// `ProjectSet` is a plan that evaluate a series of
/// set-returning functions, zip the result together,
/// and return the joined result with input relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectSet {
    pub srfs: Vec<ScalarItem>,
}

impl ProjectSet {
    pub fn derive_project_set_stats(&self, input_stat: &mut StatInfo) -> Result<Arc<StatInfo>> {
        let input_max_cardinality = input_stat.cardinality_upper_bound();
        // ProjectSet is set-returning functions, precise_cardinality set None
        input_stat.statistics.precise_cardinality = None;
        // We assume that the SRF function will expand by at least x3 per row.
        input_stat.cardinality *= 3.0;
        // The expected expansion factor is not an upper bound. Without a
        // proven per-row SRF result cap, every non-empty/unknown input remains
        // unbounded for broadcast-risk purposes.
        input_stat.max_cardinality = if input_max_cardinality == 0.0 {
            0.0
        } else {
            f64::INFINITY
        };
        Ok(Arc::new(input_stat.clone()))
    }
}

impl Operator for ProjectSet {
    fn rel_op(&self) -> RelOp {
        RelOp::ProjectSet
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        Box::new(self.srfs.iter().map(|expr| &expr.scalar))
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<RelationalProperty>> {
        let child_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = child_prop.output_columns.clone();
        for srf in &self.srfs {
            output_columns.insert(srf.index);
        }

        // Derive used columns
        let mut used_columns = child_prop.used_columns.clone();
        let mut srf_used_columns = ColumnSet::new();
        for srf in &self.srfs {
            srf.scalar.collect_used_columns(&mut srf_used_columns);
        }
        used_columns.extend(srf_used_columns.iter().copied());

        // Derive outer columns
        let mut outer_columns = child_prop.outer_columns.clone();
        outer_columns.extend(
            srf_used_columns
                .difference(&child_prop.output_columns)
                .copied(),
        );

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
        self.derive_project_set_stats(&mut input_stat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::ir::Statistics;

    #[test]
    fn test_project_set_keeps_unknown_expansion_unbounded() -> Result<()> {
        let project_set = ProjectSet { srfs: vec![] };
        let mut input = StatInfo {
            cardinality: 10.0,
            max_cardinality: 100.0,
            statistics: Statistics::default(),
        };

        let stat = project_set.derive_project_set_stats(&mut input)?;

        assert_eq!(stat.cardinality, 30.0);
        assert_eq!(stat.max_cardinality, f64::INFINITY);
        Ok(())
    }

    #[test]
    fn test_project_set_preserves_proven_empty_bound() -> Result<()> {
        let project_set = ProjectSet { srfs: vec![] };
        let mut input = StatInfo {
            cardinality: 0.0,
            max_cardinality: 0.0,
            statistics: Statistics::default(),
        };

        let stat = project_set.derive_project_set_stats(&mut input)?;

        assert_eq!(stat.cardinality, 0.0);
        assert_eq!(stat.max_cardinality, 0.0);
        Ok(())
    }
}
