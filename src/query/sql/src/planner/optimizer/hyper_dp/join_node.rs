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

use databend_common_exception::Result;

use crate::optimizer::hyper_dp::join_relation::JoinRelation;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug)]
pub struct JoinNode {
    pub join_type: JoinType,
    pub leaves: Arc<Vec<IndexType>>,
    pub children: Arc<Vec<JoinNode>>,
    pub join_conditions: Arc<Vec<(ScalarExpr, ScalarExpr)>>,
    pub cost: f64,
    // Cache cardinality/s_expr after computing.
    pub cardinality: Option<f64>,
    pub s_expr: Option<SExpr>,
}

impl JoinNode {
    pub fn cardinality(&mut self, relations: &[JoinRelation]) -> Result<f64> {
        if let Some(card) = self.cardinality {
            return Ok(card);
        }
        let s_expr = if let Some(s_expr) = &self.s_expr {
            s_expr
        } else {
            self.s_expr = Some(self.s_expr(relations));
            self.s_expr.as_ref().unwrap()
        };
        let rel_expr = RelExpr::with_s_expr(s_expr);
        let card = rel_expr.derive_cardinality()?.cardinality;
        self.cardinality = Some(card);
        Ok(card)
    }

    pub fn set_cost(&mut self, cost: f64) {
        self.cost = cost;
    }

    pub fn s_expr(&self, join_relations: &[JoinRelation]) -> SExpr {
        // Traverse JoinNode
        if self.children.is_empty() {
            // The node is leaf, get relation for `join_relations`
            let idx = self.leaves[0];
            let relation = join_relations[idx].s_expr();
            return relation;
        }

        // The node is join
        // Split `join_conditions` to `left_conditions` and `right_conditions`
        let left_conditions = self
            .join_conditions
            .iter()
            .map(|(l, _)| l.clone())
            .collect();
        let right_conditions = self
            .join_conditions
            .iter()
            .map(|(_, r)| r.clone())
            .collect();
        let rel_op = RelOperator::Join(Join {
            left_conditions,
            right_conditions,
            non_equi_conditions: vec![],
            join_type: self.join_type.clone(),
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
        });
        let children = self
            .children
            .iter()
            .map(|child| {
                if let Some(s_expr) = &child.s_expr {
                    Arc::new(s_expr.clone())
                } else {
                    Arc::new(child.s_expr(join_relations))
                }
            })
            .collect::<Vec<_>>();
        SExpr::create(Arc::new(rel_op), children, None, None, None)
    }
}
