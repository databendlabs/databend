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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataSchemaRef;

use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::ScalarExpr;

// Constant table is a table with constant values.
#[derive(Clone, Debug, PartialEq, Eq)]
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

impl std::hash::Hash for ExpressionScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expression_scan_index.hash(state);
        for row in self.values.iter() {
            for scalar in row {
                scalar.hash(state);
            }
        }
        self.num_scalar_columns.hash(state);
        self.cache_index.hash(state);
        self.column_indexes.hash(state);
        self.data_types.hash(state);
    }
}

impl Operator for ExpressionScan {
    fn rel_op(&self) -> RelOp {
        RelOp::ExpressionScan
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.column_indexes.clone().into_iter().collect(),
            outer_columns: self.used_columns()?,
            used_columns: self.used_columns()?,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: self.values.len() as f64,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: Default::default(),
            },
        }))
    }
}
