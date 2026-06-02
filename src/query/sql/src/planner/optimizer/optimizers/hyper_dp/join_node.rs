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

use crate::ScalarExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOperator;

#[derive(Clone)]
pub struct JoinNode {
    pub join_type: JoinType,
    pub children: Option<Arc<[JoinNode; 2]>>,
    pub join_conditions: Arc<Vec<(ScalarExpr, ScalarExpr)>>,
    pub cost: f64,
    // Cache cardinality/s_expr after computing.
    pub cardinality: Option<f64>,
    pub s_expr: Option<SExpr>,
}

impl JoinNode {
    pub fn cardinality(&mut self) -> Result<f64> {
        if let Some(card) = self.cardinality {
            return Ok(card);
        }
        let s_expr = if let Some(s_expr) = &self.s_expr {
            s_expr
        } else {
            self.s_expr.insert(self.s_expr())
        };

        let card = s_expr.derive_cardinality()?.cardinality;
        self.cardinality = Some(card);
        Ok(card)
    }

    pub fn join_s_expr(
        join_type: JoinType,
        children: &[JoinNode; 2],
        join_conditions: &[(ScalarExpr, ScalarExpr)],
    ) -> SExpr {
        let left_conditions = join_conditions.iter().map(|(l, _)| l.clone()).collect();
        let right_conditions = join_conditions.iter().map(|(_, r)| r.clone()).collect();
        let rel_op = RelOperator::Join(Join {
            equi_conditions: JoinEquiCondition::new_conditions(
                left_conditions,
                right_conditions,
                vec![],
            ),
            non_equi_conditions: vec![],
            join_type,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
            build_side_cache_info: None,
        });
        let children = children
            .iter()
            .map(|child| {
                if let Some(s_expr) = &child.s_expr {
                    Arc::new(s_expr.clone())
                } else {
                    Arc::new(child.s_expr())
                }
            })
            .collect::<Vec<_>>();
        SExpr::create(Arc::new(rel_op), children, None, None, None)
    }

    pub fn s_expr(&self) -> SExpr {
        // Traverse JoinNode
        match &self.children {
            None => {
                // Leaf nodes are initialized from `JoinRelation`, so they must carry their SExpr.
                self.s_expr.as_ref().unwrap().clone()
            }
            Some(children) => Self::join_s_expr(
                self.join_type,
                children.as_ref(),
                self.join_conditions.as_ref(),
            ),
        }
    }
}
