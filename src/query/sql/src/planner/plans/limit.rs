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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Limit {
    pub limit: Option<usize>,
    pub offset: usize,
}

impl Operator for Limit {
    fn rel_op(&self) -> RelOp {
        RelOp::Limit
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

impl PhysicalOperator for Limit {
    fn derive_physical_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn compute_required_prop_child<'a>(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr<'a>,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        required.distribution = Distribution::Serial;
        Ok(required)
    }
}

impl LogicalOperator for Limit {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        Ok(RelationalProperty {
            output_columns: input_prop.output_columns,
            outer_columns: input_prop.outer_columns,
            used_columns: input_prop.used_columns,
            cardinality: match self.limit {
                Some(limit) if (limit as f64) < input_prop.cardinality => limit as f64,
                _ => input_prop.cardinality,
            },
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: Default::default(),
                is_accurate: false,
            },
        })
    }

    fn used_columns<'a>(&self) -> Result<ColumnSet> {
        Ok(ColumnSet::new())
    }
}
