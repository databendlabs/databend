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

use crate::optimizer::hyper_dp::DPhpy;
use crate::optimizer::RelExpr;
use crate::plans::JoinType;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug)]
pub struct JoinNode {
    pub join_type: JoinType,
    pub leaves: Vec<IndexType>,
    pub children: Vec<JoinNode>,
    pub join_conditions: Vec<(ScalarExpr, ScalarExpr)>,
    pub cost: f64,
    // Cache cardinality after computing.
    pub cardinality: Option<f64>,
}

impl JoinNode {
    pub fn cardinality(&mut self, dphpy: &DPhpy) -> Result<f64> {
        let s_expr = dphpy.s_expr(self);
        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let card = rel_expr.derive_relational_prop()?.cardinality;
        self.cardinality = Some(card);
        Ok(card)
    }
}
