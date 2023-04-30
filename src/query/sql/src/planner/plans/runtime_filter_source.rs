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

use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;

use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(
    Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, Ord, PartialOrd,
)]
pub struct RuntimeFilterId {
    id: String,
}

impl RuntimeFilterId {
    pub fn new(id: IndexType) -> Self {
        RuntimeFilterId { id: id.to_string() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeFilterSource {
    pub left_runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
    pub right_runtime_filters: BTreeMap<RuntimeFilterId, ScalarExpr>,
}

impl RuntimeFilterSource {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for expr in self
            .left_runtime_filters
            .iter()
            .chain(self.right_runtime_filters.iter())
        {
            used_columns.extend(expr.1.used_columns());
        }
        Ok(used_columns)
    }
}

impl Operator for RuntimeFilterSource {
    fn rel_op(&self) -> RelOp {
        RelOp::RuntimeFilterSource
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<RelationalProperty> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        // Derive output columns
        let output_columns = left_prop.output_columns.clone();
        // Derive outer columns
        let outer_columns = left_prop.outer_columns;

        Ok(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns: self.used_columns()?,
        })
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        todo!()
    }

    fn derive_cardinality(&self, rel_expr: &RelExpr) -> Result<StatInfo> {
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        Ok(StatInfo {
            cardinality: stat_info.cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: stat_info.statistics.column_stats,
            },
        })
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        todo!()
    }
}
