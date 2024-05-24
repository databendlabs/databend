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
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AsyncFunction {
    pub func_name: String,
    pub display_name: String,
    pub arguments: Vec<String>,
    pub return_type: DataType,
    pub index: IndexType,
}

impl Operator for AsyncFunction {
    fn rel_op(&self) -> RelOp {
        RelOp::AsyncFunction
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        output_columns.insert(self.index);

        // Derive outer columns
        let mut outer_columns = input_prop.outer_columns.clone();
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

        // Derive used columns
        let used_columns = input_prop.used_columns.clone();

        // Derive orderings
        let orderings = input_prop.orderings.clone();
        let partition_orderings = input_prop.partition_orderings.clone();

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings,
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
