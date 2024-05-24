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

use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sort {
    pub items: Vec<SortItem>,
    pub limit: Option<usize>,

    /// If the sort plan is after the exchange plan.
    /// It's [None] if the sorting plan is in single node mode.
    pub after_exchange: Option<bool>,

    /// The columns needed by the plan after the sort plan.
    /// It's used to build a projection operation before building the sort operator.
    pub pre_projection: Option<Vec<IndexType>>,

    /// If sort is for window clause, we need the input to exchange by partitions
    pub window_partition: Vec<ScalarItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SortItem {
    pub index: IndexType,
    pub asc: bool,
    pub nulls_first: bool,
}

impl Operator for Sort {
    fn rel_op(&self) -> RelOp {
        RelOp::Sort
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let input_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if input_physical_prop.distribution == Distribution::Serial
            || self.window_partition.is_empty()
        {
            return Ok(input_physical_prop);
        }

        let partition_by = self
            .window_partition
            .iter()
            .map(|s| s.scalar.clone())
            .collect();
        Ok(PhysicalProperty {
            distribution: Distribution::Hash(partition_by),
        })
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        required.distribution = if self.window_partition.is_empty() {
            Distribution::Serial
        } else {
            let partition_by = self
                .window_partition
                .iter()
                .map(|s| s.scalar.clone())
                .collect();
            Distribution::Hash(partition_by)
        };
        Ok(required)
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let distribution = if self.window_partition.is_empty() {
            RequiredProperty {
                distribution: Distribution::Serial,
            }
        } else {
            let partition_by = self
                .window_partition
                .iter()
                .map(|s| s.scalar.clone())
                .collect();
            RequiredProperty {
                distribution: Distribution::Hash(partition_by),
            }
        };

        Ok(vec![vec![distribution]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let output_columns = input_prop.output_columns.clone();
        let outer_columns = input_prop.outer_columns.clone();
        let used_columns = input_prop.used_columns.clone();

        // Derive orderings
        let orderings = self.items.clone();
        let partition_orderings = if !self.window_partition.is_empty() {
            Some((self.window_partition.clone(), orderings.clone()))
        } else {
            None
        };

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
}
