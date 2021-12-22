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

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::Result;

use crate::sql::optimizer::memo::Memo;
use crate::sql::optimizer::pattern_extractor::PatternExtractor;
use crate::sql::optimizer::rule::RulePtr;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::s_expr::PlanPtr;
use crate::sql::optimizer::SExpr;
use crate::sql::IndexType;

/// `MExpr` is abbreviation of multiple expression, which is the representation of relational
/// expressions inside `Memo`.
#[derive(Clone)]
pub struct MExpr {
    group_index: IndexType,
    plan: PlanPtr,
    children: Vec<IndexType>,
}

impl MExpr {
    pub fn create(group_index: IndexType, plan: PlanPtr, children: Vec<IndexType>) -> Self {
        MExpr {
            group_index,
            plan,
            children,
        }
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn group_index(&self) -> IndexType {
        self.group_index
    }

    pub fn plan(&self) -> PlanPtr {
        self.plan.clone()
    }

    pub fn children(&self) -> &Vec<IndexType> {
        &self.children
    }

    /// Doesn't check if children are matched
    pub fn match_pattern(&self, _memo: &Memo, pattern: &SExpr) -> bool {
        if pattern.is_pattern() {
            return true;
        }

        if self.arity() != pattern.arity() {
            return false;
        }

        self.plan.kind_eq(&pattern.plan())
    }

    pub fn apply_rule(
        &self,
        memo: &Memo,
        rule: &RulePtr,
        transform_state: &mut TransformState,
    ) -> Result<()> {
        let mut extractor = PatternExtractor::create();
        let exprs = extractor.extract(memo, self, rule.pattern());

        for expr in exprs.iter() {
            rule.apply(expr, transform_state)?;
        }

        Ok(())
    }
}

impl Debug for MExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        write!(f, "plan: {:?}, ", &self.plan)?;
        write!(f, "children: {:?}", &self.children)?;
        write!(f, "}}")?;

        Ok(())
    }
}
