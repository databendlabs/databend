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

use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::ScalarExpr;

pub struct DeduplicateJoinConditionOptimizer {
    pub scalar_expr_index: HashMap<ScalarExpr, usize>,
    pub parent: HashMap<usize, usize>,
    pub num_scalar_expr: usize,
}

impl DeduplicateJoinConditionOptimizer {
    pub fn new() -> Self {
        DeduplicateJoinConditionOptimizer {
            scalar_expr_index: HashMap::new(),
            parent: HashMap::new(),
            num_scalar_expr: 0,
        }
    }

    pub fn run(mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.deduplicate(s_expr)
    }

    pub fn deduplicate(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            RelOperator::Join(join) if join.join_type == JoinType::Inner => {
                self.deduplicate_join_conditions(s_expr, join)
            }
            _ => self.deduplicate_children(s_expr),
        }
    }

    fn deduplicate_join_conditions(&mut self, s_expr: &SExpr, join: &Join) -> Result<SExpr> {
        debug_assert!(join.join_type == JoinType::Inner);

        let left = self.deduplicate(s_expr.child(0)?)?;
        let right = self.deduplicate(s_expr.child(1)?)?;
        let mut join = join.clone();
        let mut new_left_conditions = Vec::new();
        let mut new_right_conditions = Vec::new();
        for (left_condition, right_condition) in join
            .left_conditions
            .iter()
            .zip(join.right_conditions.iter())
        {
            let left_index = self.get_scalar_expr_index(left_condition);
            let right_index = self.get_scalar_expr_index(right_condition);
            let left_parent_index = self.find(left_index);
            let right_parent_index = self.find(right_index);
            if left_parent_index != right_parent_index {
                *self.parent.get_mut(&right_parent_index).unwrap() = left_parent_index;
                new_left_conditions.push(left_condition.clone());
                new_right_conditions.push(right_condition.clone());
            }
        }
        if new_left_conditions.len() != join.left_conditions.len() {
            join.left_conditions = new_left_conditions;
            join.right_conditions = new_right_conditions;
        }
        let s_expr = s_expr.replace_plan(Arc::new(RelOperator::Join(join)));
        Ok(s_expr.replace_children(vec![Arc::new(left), Arc::new(right)]))
    }

    pub fn deduplicate_children(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(s_expr.children().len());
        for child in s_expr.children() {
            let child = self.deduplicate(child)?;
            children.push(Arc::new(child));
        }
        Ok(s_expr.replace_children(children))
    }

    fn get_scalar_expr_index(&mut self, scalar_expr: &ScalarExpr) -> usize {
        match self.scalar_expr_index.get(scalar_expr) {
            Some(index) => *index,
            None => {
                let index = self.num_scalar_expr;
                self.scalar_expr_index.insert(scalar_expr.clone(), index);
                self.num_scalar_expr += 1;
                index
            }
        }
    }

    fn find(&mut self, index: usize) -> usize {
        match self.parent.get(&index) {
            Some(parent_index) => {
                if index != *parent_index {
                    let new_parent_index = self.find(*parent_index);
                    self.parent.insert(index, new_parent_index);
                    new_parent_index
                } else {
                    index
                }
            }
            None => {
                self.parent.insert(index, index);
                index
            }
        }
    }
}
