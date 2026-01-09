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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::ColumnSet;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SelectivityEstimator;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    pub predicates: Vec<ScalarExpr>,
}

impl Filter {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(self
            .predicates
            .iter()
            .map(|scalar| scalar.used_columns())
            .fold(ColumnSet::new(), |acc, x| acc.union(&x).cloned().collect()))
    }
}

impl Operator for Filter {
    fn rel_op(&self) -> RelOp {
        RelOp::Filter
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        Box::new(self.predicates.iter())
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;
        let output_columns = input_prop.output_columns.clone();

        // Derive outer columns
        let mut outer_columns = input_prop.outer_columns.clone();
        for scalar in self.predicates.iter() {
            let used_columns = scalar.used_columns();
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
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        // Derive cardinality
        let mut sb = SelectivityEstimator::new(
            stat_info.statistics.column_stats.clone(),
            stat_info.cardinality,
        );
        let cardinality = sb.apply(&self.predicates)?;
        // Derive column statistics
        let column_stats = if cardinality == 0.0 {
            HashMap::new()
        } else {
            sb.into_column_stats()
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
            },
        }))
    }
}
