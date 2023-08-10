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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Column;
use common_expression::DataSchemaRef;
use itertools::Itertools;

use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;

// Constant table is a table with constant values.
#[derive(Clone, Debug)]
pub struct ConstantTableScan {
    pub values: Vec<Column>,
    pub num_rows: usize,
    pub schema: DataSchemaRef,
    pub columns: ColumnSet,
}

impl ConstantTableScan {
    pub fn prune_columns(&self, columns: ColumnSet) -> Self {
        let mut projection = columns
            .iter()
            .map(|index| self.schema.index_of(&index.to_string()).unwrap())
            .collect::<Vec<_>>();
        projection.sort();

        let schema = self.schema.project(&projection);
        let values = projection
            .iter()
            .map(|idx| self.values[*idx].clone())
            .collect();

        ConstantTableScan {
            values,
            schema: Arc::new(schema),
            columns,
            num_rows: self.num_rows,
        }
    }

    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(self.columns.clone())
    }
}

impl PartialEq for ConstantTableScan {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
    }
}

impl Eq for ConstantTableScan {}

impl std::hash::Hash for ConstantTableScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for column in self.columns.iter().sorted() {
            column.hash(state);
        }
    }
}

impl Operator for ConstantTableScan {
    fn rel_op(&self) -> RelOp {
        RelOp::ConstantTableScan
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            used_columns: self.columns.clone(),
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn derive_cardinality(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: self.num_rows as f64,
            // TODO
            statistics: Default::default(),
        }))
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
}
