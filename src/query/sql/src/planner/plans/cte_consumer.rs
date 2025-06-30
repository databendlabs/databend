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

use std::hash::Hash;

use databend_common_expression::DataSchemaRef;

use crate::plans::Operator;
use crate::plans::RelOp;
use std::hash::Hasher;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CTEConsumer {
    pub cte_name: String,
    pub cte_schema: DataSchemaRef,
}

impl Hash for CTEConsumer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cte_name.hash(state);
    }
}

impl Operator for CTEConsumer {
    fn rel_op(&self) -> RelOp {
        RelOp::CTEConsumer
    }

    // fn arity(&self) -> usize {
    //     0
    // }

    // fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
    //     Ok(Arc::new(RelationalProperty {
    //         output_columns: self.used_columns()?,
    //         outer_columns: ColumnSet::new(),
    //         used_columns: self.used_columns()?,
    //         orderings: vec![],
    //         partition_orderings: None,
    //     }))
    // }

    // fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
    //     Ok(PhysicalProperty {
    //         distribution: Distribution::Serial,
    //     })
    // }

    // fn compute_required_prop_child(
    //     &self,
    //     _ctx: Arc<dyn TableContext>,
    //     _rel_expr: &RelExpr,
    //     _child_index: usize,
    //     _required: &RequiredProperty,
    // ) -> Result<RequiredProperty> {
    //     Err(ErrorCode::Internal(
    //         "Cannot compute required property for CTEConsumer".to_string(),
    //     ))
    // }
}
