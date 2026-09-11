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

use super::WindowPartition;
use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatContext;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::cap_stat_info_by_rows;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sort {
    pub items: Vec<SortItem>,
    pub limit: Option<usize>,

    pub after_exchange: Option<bool>,

    /// The columns needed by the plan after the sort plan.
    /// It's used to build a projection operation before building the sort operator.
    pub pre_projection: Option<Vec<Symbol>>,

    /// If sort is for window clause, we need the input to exchange by partitions
    pub window_partition: Option<WindowPartition>,
}

impl Sort {
    pub fn used_columns(&self) -> ColumnSet {
        let mut used_columns: ColumnSet = self.items.iter().map(|item| item.index).collect();
        if let Some(window) = &self.window_partition {
            for item in &window.partition_by {
                used_columns.insert(item.index);
                item.scalar.collect_used_columns(&mut used_columns);
            }
            window.func.collect_used_columns(&mut used_columns);
        }
        used_columns
    }

    pub fn replace_column(&mut self, old: Symbol, new: Symbol) {
        for item in &mut self.items {
            if item.index == old {
                item.index = new
            }
        }

        if let Some(projection) = &mut self.pre_projection {
            for i in projection {
                if *i == old {
                    *i = new
                }
            }
        }

        if let Some(window) = &mut self.window_partition {
            for item in &mut window.partition_by {
                if item.index == old {
                    item.index = new;
                }
                let _ = item.scalar.replace_column(old, new);
            }
            window.func.replace_column(old, new);
        }
    }

    pub fn replace_columns<F>(&mut self, mut replace: F) -> Result<()>
    where F: FnMut(Symbol) -> Result<Symbol> {
        for item in &mut self.items {
            item.index = replace(item.index)?;
        }

        if let Some(projection) = &mut self.pre_projection {
            for index in projection {
                *index = replace(*index)?;
            }
        }

        if let Some(window) = &mut self.window_partition {
            for item in &mut window.partition_by {
                item.index = replace(item.index)?;
                item.scalar.replace_columns(&mut replace)?;
            }
            window.func.replace_columns(&mut replace)?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SortItem {
    pub index: Symbol,
    pub asc: bool,
    pub nulls_first: bool,
}

impl Operator for Sort {
    fn rel_op(&self) -> RelOp {
        RelOp::Sort
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        let partition_items = self.window_partition.iter().flat_map(|partition| {
            partition
                .partition_by
                .iter()
                .map(|item| &item.scalar)
                .chain(partition.func.scalar_expr_iter())
        });
        Box::new(partition_items)
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let input_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        if input_physical_prop.distribution == Distribution::Serial {
            return Ok(input_physical_prop);
        }
        let Some(window) = &self.window_partition else {
            return Ok(input_physical_prop);
        };

        let partition_by = window
            .partition_by
            .iter()
            .map(|s| s.scalar.clone())
            .collect();
        Ok(PhysicalProperty {
            distribution: Distribution::GlobalHash(partition_by),
        })
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        required.distribution = Distribution::Serial;

        let Some(window) = &self.window_partition else {
            return Ok(required);
        };

        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        // Can't merge to shuffle
        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(required);
        }

        let partition_by = window
            .partition_by
            .iter()
            .map(|s| s.scalar.clone())
            .collect();
        required.distribution = Distribution::GlobalHash(partition_by);

        Ok(required)
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let mut required = required.clone();
        required.distribution = Distribution::Serial;

        let Some(window) = &self.window_partition else {
            return Ok(vec![vec![required]]);
        };

        // Can't merge to shuffle
        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(vec![vec![required]]);
        }

        let partition_by = window
            .partition_by
            .iter()
            .map(|s| s.scalar.clone())
            .collect();

        required.distribution = Distribution::GlobalHash(partition_by);
        Ok(vec![vec![required]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let output_columns = input_prop.output_columns.clone();
        let mut outer_columns =
            self.derive_outer_columns(input_prop.outer_columns.clone(), &input_prop.output_columns);
        outer_columns.extend(
            self.used_columns()
                .difference(&input_prop.output_columns)
                .copied(),
        );
        let mut used_columns = input_prop.used_columns.clone();
        used_columns.extend(self.used_columns());

        // Derive orderings
        let orderings = self.items.clone();

        let (orderings, partition_orderings) = match &self.window_partition {
            Some(window) => (
                vec![],
                Some((window.partition_by.clone(), orderings.clone())),
            ),
            None => (self.items.clone(), None),
        };

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr, stat_ctx: &StatContext) -> Result<Arc<StatInfo>> {
        let input = rel_expr.derive_cardinality_child(0, stat_ctx)?;
        let Some(limit) = self.limit else {
            return Ok(input);
        };
        Ok(Arc::new(cap_stat_info_by_rows(
            input.as_ref().clone(),
            limit,
        )))
    }
}
