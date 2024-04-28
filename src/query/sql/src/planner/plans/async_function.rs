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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use super::Operator;
use super::RelOp;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AsyncFunction {
    pub func_name: String,
    pub display_name: String,
    pub arguments: Vec<String>,
    pub return_type: DataType,
    pub index: IndexType,
}

impl AsyncFunction {
    fn output_columns(&self) -> ColumnSet {
        let mut output_columns = ColumnSet::new();
        output_columns.insert(self.index);
        output_columns
    }
}

impl Operator for AsyncFunction {
    fn rel_op(&self) -> RelOp {
        RelOp::AsyncFunction
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.output_columns(),
            ..Default::default()
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: 0.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::new(),
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
