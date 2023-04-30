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

use common_catalog::table_context::TableContext;
use common_exception::Result;

use crate::optimizer::ColumnSet;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SelectivityEstimator;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::optimizer::MAX_SELECTIVITY;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Filter {
    pub predicates: Vec<ScalarExpr>,
    // True if the plan represents having, else the plan represents where
    pub is_having: bool,
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

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<RelationalProperty> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;
        let output_columns = input_prop.output_columns;

        // Derive outer columns
        let mut outer_columns = input_prop.outer_columns;
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
        used_columns.extend(input_prop.used_columns);

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
        })
    }

    fn derive_cardinality(&self, rel_expr: &RelExpr) -> Result<StatInfo> {
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        let (input_cardinality, mut statistics) = (stat_info.cardinality, stat_info.statistics);
        // Derive cardinality
        let mut sb = SelectivityEstimator::new(&mut statistics);
        let mut selectivity = MAX_SELECTIVITY;
        for pred in self.predicates.iter() {
            // Compute selectivity for each conjunction
            selectivity *= sb.compute_selectivity(pred, true)?;
        }
        let cardinality = input_cardinality * selectivity;

        // Derive column statistics
        let column_stats = if cardinality == 0.0 {
            HashMap::new()
        } else {
            for (_, column_stat) in statistics.column_stats.iter_mut() {
                if cardinality < input_cardinality {
                    column_stat.histogram = None;
                    column_stat.ndv = (column_stat.ndv * selectivity).ceil();
                }
            }
            statistics.column_stats
        };
        Ok(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
            },
        })
    }
}
