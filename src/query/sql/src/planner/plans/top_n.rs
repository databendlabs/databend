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
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::SortItem;

/// TopN operator: the fusion of `Limit` and `Sort` produced by
/// `RulePushDownLimitSort` when `limit + offset` is within the
/// push-down threshold.
///
/// Semantics: the top `limit` rows ordered by `items`, after skipping
/// `offset` rows. The candidate capacity of the partial stage is
/// `limit + offset`; `offset` is only applied at the final stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopN {
    pub items: Vec<SortItem>,
    pub limit: usize,
    pub offset: usize,

    /// Lazy columns absorbed from the `Limit` operator, used to build a
    /// `RowFetch` above the final TopN stage.
    pub lazy_columns: ColumnSet,

    /// Distributed marker, mirroring `Sort::after_exchange`:
    /// - `None`: single-node plan.
    /// - `Some(false)`: partial stage below the exchange.
    /// - `Some(true)`: final stage above the exchange.
    pub after_exchange: Option<bool>,
}

impl TopN {
    pub fn used_columns(&self) -> ColumnSet {
        self.items.iter().map(|item| item.index).collect()
    }

    /// The candidate capacity of the partial stage.
    pub fn candidate_count(&self) -> usize {
        self.limit.saturating_add(self.offset)
    }

    pub fn without_lazy_columns(&self) -> TopN {
        TopN {
            lazy_columns: Default::default(),
            ..self.clone()
        }
    }
}

impl Operator for TopN {
    fn rel_op(&self) -> RelOp {
        RelOp::TopN
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

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        Ok(vec![vec![RequiredProperty {
            distribution: Distribution::Serial,
        }]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        Ok(Arc::new(RelationalProperty {
            output_columns: input_prop.output_columns.clone(),
            outer_columns: input_prop.outer_columns.clone(),
            used_columns: input_prop.used_columns.clone(),
            orderings: self.items.clone(),
            partition_orderings: None,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        let partial = self.after_exchange == Some(false);
        let output_rows = if partial {
            self.candidate_count()
        } else {
            self.limit
        };
        let cardinality = stat_info.cardinality.min(output_rows as f64);
        let precise_cardinality = stat_info.statistics.precise_cardinality.map(|rows| {
            if partial {
                rows.min(self.candidate_count() as u64)
            } else {
                rows.saturating_sub(self.offset as u64)
                    .min(self.limit as u64)
            }
        });

        Ok(Arc::new(StatInfo {
            cardinality,
            max_cardinality: stat_info
                .max_cardinality
                .max(stat_info.cardinality)
                .min(output_rows as f64),
            statistics: Statistics {
                precise_cardinality,
                column_stats: Default::default(),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }))
    }
}
