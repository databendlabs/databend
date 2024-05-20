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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataSchemaRef;

use crate::optimizer::ColumnSet;
use crate::optimizer::ColumnStatSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::ScalarExpr;

// Constant table is a table with constant values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ExpressionScan {
    pub expression_scan_index: usize,
    pub values: Vec<Vec<ScalarExpr>>,
    pub num_scalar_columns: usize,
    pub cache_index: usize,
    pub column_indexes: Vec<usize>,
    pub data_types: Vec<DataType>,
    pub schema: DataSchemaRef,
}

impl ExpressionScan {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut columns = HashSet::new();
        for row in self.values.iter() {
            for value in row {
                columns.extend(value.used_columns());
            }
        }
        Ok(columns)
    }

    pub fn remove_cache_column(&mut self, index: usize) {
        for row in self.values.iter_mut() {
            row.remove(index);
        }
        self.column_indexes.remove(index);
        self.data_types.remove(index);
    }
}

impl Operator for ExpressionScan {
    fn rel_op(&self) -> RelOp {
        RelOp::ExpressionScan
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.column_indexes.clone().into_iter().collect(),
            outer_columns: self.used_columns()?,
            used_columns: self.used_columns()?,
            orderings: vec![],
        }))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: 0.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: Default::default(),
            },
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
