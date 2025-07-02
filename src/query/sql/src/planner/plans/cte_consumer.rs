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
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;

use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;

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

    /// Get arity of this operator
    fn arity(&self) -> usize {
        0
    }

    /// Derive statistics information
    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        rel_expr.derive_cardinality()
    }
}
