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

use crate::binder::WindowOrderByInfo;
use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::plans::WindowFuncFrame;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    // aggregate scalar expressions, such as: sum(col1), count(*);
    pub aggregate_function: ScalarItem,
    // partition by scalar expressions
    pub partition_by: Vec<ScalarItem>,
    // order by
    pub order_by: Vec<WindowOrderByInfo>,
    // window frames
    pub frame: WindowFuncFrame,
}

impl Window {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();

        used_columns.insert(self.aggregate_function.index);
        used_columns.extend(self.aggregate_function.scalar.used_columns());

        for part in self.partition_by.iter() {
            used_columns.insert(part.index);
            used_columns.extend(part.scalar.used_columns())
        }

        for sort in self.order_by.iter() {
            used_columns.insert(sort.order_by_item.index);
            used_columns.extend(sort.order_by_item.scalar.used_columns())
        }

        Ok(used_columns)
    }
}

impl Operator for Window {
    fn rel_op(&self) -> RelOp {
        RelOp::Window
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
        let mut required = required.clone();
        required.distribution = Distribution::Serial;
        Ok(required)
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<RelationalProperty> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let output_columns = ColumnSet::from([self.aggregate_function.index]);

        // Derive outer columns
        let outer_columns = input_prop
            .outer_columns
            .difference(&output_columns)
            .cloned()
            .collect();

        let cardinality = if self.partition_by.is_empty() {
            // Scalar aggregation
            1.0
        } else if self.partition_by.iter().any(|item| {
            input_prop
                .statistics
                .column_stats
                .get(&item.index)
                .is_none()
        }) {
            input_prop.cardinality
        } else {
            // A upper bound
            let res = self.partition_by.iter().fold(1.0, |acc, item| {
                let item_stat = input_prop.statistics.column_stats.get(&item.index).unwrap();
                acc * item_stat.ndv
            });
            // To avoid res is very large
            f64::min(res, input_prop.cardinality)
        };

        let precise_cardinality = if self.partition_by.is_empty() {
            Some(1)
        } else {
            None
        };

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns);
        let column_stats = input_prop.statistics.column_stats;
        let is_accurate = input_prop.statistics.is_accurate;

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats,
                is_accurate,
            },
        })
    }
}
