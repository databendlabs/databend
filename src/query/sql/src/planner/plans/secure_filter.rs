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
use crate::IndexType;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SelectivityEstimator;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecureFilter {
    pub predicates: Vec<ScalarExpr>,
    pub table_index: IndexType,
}

impl SecureFilter {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(self
            .predicates
            .iter()
            .map(|scalar| scalar.used_columns())
            .fold(ColumnSet::new(), |acc, x| acc.union(&x).cloned().collect()))
    }
}

impl Operator for SecureFilter {
    fn rel_op(&self) -> RelOp {
        RelOp::SecureFilter
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

        // For SecureFilter, we apply the selectivity calculation like normal Filter
        // but we hide/suppress column statistics for sensitive columns

        // Apply selectivity calculation
        let mut sb = SelectivityEstimator::new(
            stat_info.statistics.column_stats.clone(),
            stat_info.cardinality,
        );
        let cardinality = sb.apply(&self.predicates)?;
        Ok(Arc::new(StatInfo {
            cardinality,
            // SECURITY: Hide column statistics for SecureFilter to prevent data leakage
            // This is a key security feature - we return empty column stats to prevent
            // inference attacks based on statistical information
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::new(),
            },
        }))
    }
}
