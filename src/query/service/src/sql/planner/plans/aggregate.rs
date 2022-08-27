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

use common_exception::Result;

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::Distribution;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;
use crate::sql::plans::ScalarItem;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum AggregateMode {
    Partial,
    Final,

    // TODO(leiysky): this mode is only used for preventing recursion of
    // RuleSplitAggregate, find a better way.
    Initial,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    pub mode: AggregateMode,
    // group by scalar expressions, such as: group by col1 asc, col2 desc;
    pub group_items: Vec<ScalarItem>,
    // aggregate scalar expressions, such as: sum(col1), count(*);
    pub aggregate_functions: Vec<ScalarItem>,
    // True if the plan is generated from distinct, else the plan is a normal aggregate;
    pub from_distinct: bool,
}

impl Operator for Aggregate {
    fn rel_op(&self) -> RelOp {
        RelOp::Aggregate
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        Some(self)
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        Some(self)
    }
}

impl PhysicalOperator for Aggregate {
    fn derive_physical_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn compute_required_prop_child<'a>(
        &self,
        rel_expr: &RelExpr<'a>,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(required);
        }

        match self.mode {
            AggregateMode::Partial => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Any;
                } else {
                    // Group aggregation, enforce `Hash` distribution
                    required.distribution = Distribution::Hash(
                        self.group_items
                            .iter()
                            .map(|item| item.scalar.clone())
                            .collect(),
                    );
                }
            }

            AggregateMode::Final => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Serial;
                } else {
                    // Group aggregation, enforce `Hash` distribution
                    required.distribution = Distribution::Hash(
                        self.group_items
                            .iter()
                            .map(|item| item.scalar.clone())
                            .collect(),
                    );
                }
            }

            AggregateMode::Initial => unreachable!(),
        }
        Ok(required)
    }
}

impl LogicalOperator for Aggregate {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = ColumnSet::new();
        for group_item in self.group_items.iter() {
            output_columns.insert(group_item.index);
        }
        for agg in self.aggregate_functions.iter() {
            output_columns.insert(agg.index);
        }

        // Derive outer columns
        let outer_columns = input_prop
            .outer_columns
            .difference(&output_columns)
            .cloned()
            .collect();

        // Derive cardinality. We can not estimate the cardinality of an aggregate with group by, until
        // we have information about distribution of group keys. So we pass through the cardinality.
        let cardinality = if self.group_items.is_empty() {
            // Scalar aggregation
            1.0
        } else {
            input_prop.cardinality
        };

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            cardinality,
        })
    }
}
