// Copyright 2021 Datafuse Labs.
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

use std::rc::Rc;

use crate::sql::optimizer::property::RelationalProperty;
use crate::sql::IndexType;
use crate::sql::Plan;

pub type PlanPtr = Rc<Plan>;

/// `SExpr` is abbreviation of single expression, which is a tree of relational operators.
#[derive(Clone, Debug, PartialEq)]
pub struct SExpr {
    plan: PlanPtr,
    children: Vec<SExpr>,

    original_group: Option<IndexType>,
}

impl SExpr {
    pub fn create(plan: PlanPtr, children: Vec<SExpr>, original_group: Option<IndexType>) -> Self {
        SExpr {
            plan,
            children,
            original_group,
        }
    }

    pub fn create_unary(plan: PlanPtr, child: SExpr) -> Self {
        Self::create(plan, vec![child], None)
    }

    pub fn create_binary(plan: PlanPtr, left_child: SExpr, right_child: SExpr) -> Self {
        Self::create(plan, vec![left_child, right_child], None)
    }

    pub fn create_leaf(plan: PlanPtr) -> Self {
        Self::create(plan, vec![], None)
    }

    pub fn plan(&self) -> PlanPtr {
        self.plan.clone()
    }

    pub fn children(&self) -> &Vec<SExpr> {
        &self.children
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn is_pattern(&self) -> bool {
        matches!(*self.plan, Plan::Pattern)
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.original_group
    }

    pub fn match_pattern(&self, pattern: &SExpr) -> bool {
        if !pattern.plan().kind_eq(&Plan::Pattern) {
            // Pattern is plan
            if self.plan().kind_eq(&pattern.plan()) {
                return false;
            }

            if self.arity() != pattern.arity() {
                // Check if current expression has same arity with current pattern
                return false;
            }

            for (e, p) in self.children.iter().zip(pattern.children.iter()) {
                // Check children
                if !e.match_pattern(p) {
                    return false;
                }
            }
        };

        true
    }

    pub fn compute_relational_prop(&self) -> RelationalProperty {
        if self.plan.is_logical() {
            self.plan.compute_relational_prop(self).unwrap()
        } else {
            RelationalProperty::default()
        }
    }
}
