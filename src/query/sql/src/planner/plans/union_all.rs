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

use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnionAll {
    // We'll cast the output of union to the expected data type by the cast expr at runtime.
    // Left of union, output idx and the expected data type
    pub left_outputs: Vec<(IndexType, Option<ScalarExpr>)>,
    // Right of union, output idx and the expected data type
    pub right_outputs: Vec<(IndexType, Option<ScalarExpr>)>,
    // Recursive cte scan names
    // For example: `with recursive t as (select 1 as x union all select m.x+f.x from t as m, t as f where m.x < 3) select * from t`
    // The `cte_scan_names` are `m` and `f`
    pub cte_scan_names: Vec<String>,
    pub output_indexes: Vec<IndexType>,
}

impl UnionAll {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for (idx, _) in &self.left_outputs {
            used_columns.insert(*idx);
        }
        for (idx, _) in &self.right_outputs {
            used_columns.insert(*idx);
        }
        Ok(used_columns)
    }

    pub fn derive_union_stats(
        &self,
        left_stat_info: Arc<StatInfo>,
        right_stat_info: Arc<StatInfo>,
    ) -> Result<Arc<StatInfo>> {
        let cardinality = left_stat_info.cardinality + right_stat_info.cardinality;

        let precise_cardinality =
            left_stat_info
                .statistics
                .precise_cardinality
                .and_then(|left_cardinality| {
                    right_stat_info
                        .statistics
                        .precise_cardinality
                        .map(|right_cardinality| left_cardinality + right_cardinality)
                });

        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: Default::default(),
            },
        }))
    }
}

impl Operator for UnionAll {
    fn rel_op(&self) -> RelOp {
        RelOp::UnionAll
    }

    fn arity(&self) -> usize {
        2
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        // Derive output columns
        let output_columns = self.output_indexes.iter().cloned().collect();
        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns.clone();
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns.clone());
        used_columns.extend(right_prop.used_columns.clone());

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let left_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let right_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        if left_physical_prop.distribution == Distribution::Serial
            || right_physical_prop.distribution == Distribution::Serial
        {
            return Ok(PhysicalProperty {
                distribution: Distribution::Serial,
            });
        }

        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let left_stat_info = rel_expr.derive_cardinality_child(0)?;
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        self.derive_union_stats(left_stat_info, right_stat_info)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let required = required.clone();
        let left_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let right_physical_prop = rel_expr.derive_physical_prop_child(1)?;
        if left_physical_prop.distribution == Distribution::Serial
            || right_physical_prop.distribution == Distribution::Serial
            || required.distribution == Distribution::Serial
        {
            Ok(RequiredProperty {
                distribution: Distribution::Serial,
            })
        } else {
            Ok(RequiredProperty {
                distribution: Distribution::Random,
            })
        }
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        // (Any, Any)
        let mut children_required = vec![vec![
            RequiredProperty {
                distribution: Distribution::Any,
            },
            RequiredProperty {
                distribution: Distribution::Any,
            },
        ]];

        // (Serial, Serial)
        children_required.push(vec![
            RequiredProperty {
                distribution: Distribution::Serial,
            },
            RequiredProperty {
                distribution: Distribution::Serial,
            },
        ]);

        Ok(children_required)
    }
}
