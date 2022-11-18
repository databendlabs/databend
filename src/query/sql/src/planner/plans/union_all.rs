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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;

use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::Statistics;
use crate::plans::LogicalOperator;
use crate::plans::Operator;
use crate::plans::PhysicalOperator;
use crate::plans::RelOp;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnionAll {
    // Pairs of unioned columns
    pub pairs: Vec<(IndexType, IndexType)>,
}

impl Operator for UnionAll {
    fn rel_op(&self) -> RelOp {
        RelOp::UnionAll
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

impl LogicalOperator for UnionAll {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        // Derive output columns
        let mut output_columns = left_prop.output_columns;
        output_columns = output_columns
            .union(&right_prop.output_columns)
            .cloned()
            .collect();

        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns;
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();

        let cardinality = left_prop.cardinality + right_prop.cardinality;

        let precise_cardinality =
            left_prop
                .statistics
                .precise_cardinality
                .and_then(|left_cardinality| {
                    right_prop
                        .statistics
                        .precise_cardinality
                        .map(|right_cardinality| left_cardinality + right_cardinality)
                });

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns);
        used_columns.extend(right_prop.used_columns);

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: Default::default(),
                is_accurate: left_prop.statistics.is_accurate && right_prop.statistics.is_accurate,
            },
        })
    }

    fn used_columns<'a>(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for (left, right) in &self.pairs {
            used_columns.insert(*left);
            used_columns.insert(*right);
        }
        Ok(used_columns)
    }
}

impl PhysicalOperator for UnionAll {
    fn derive_physical_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn compute_required_prop_child<'a>(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr<'a>,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Ok(required.clone())
    }
}
