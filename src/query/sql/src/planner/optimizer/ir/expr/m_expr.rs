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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::ir::expr::PatternExtractor;
use crate::optimizer::ir::group::Group;
use crate::optimizer::ir::Memo;
use crate::optimizer::optimizers::rule::AppliedRules;
use crate::optimizer::optimizers::rule::RulePtr;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::RelOperator;
use crate::IndexType;

/// `MExpr` is abbreviation of multiple expression, which is the representation of relational
/// expressions inside `Memo`.
#[derive(Clone)]
pub struct MExpr {
    // index of current `Group`
    pub(crate) group_index: IndexType,
    // index of current `MExpr` within a `Group`
    pub(crate) index: IndexType,

    pub(crate) plan: Arc<RelOperator>,
    pub(crate) children: Vec<IndexType>,

    // Disable rules for current `MExpr`
    pub(crate) applied_rules: AppliedRules,
}

impl MExpr {
    pub fn new(
        group_index: IndexType,
        index: IndexType,
        plan: Arc<RelOperator>,
        children: Vec<IndexType>,
        applied_rules: AppliedRules,
    ) -> Self {
        MExpr {
            group_index,
            plan,
            children,
            index,
            applied_rules,
        }
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn child_group<'a>(&'a self, memo: &'a Memo, child_index: usize) -> Result<&'a Group> {
        let group_index = self.children.get(child_index).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "child_index {} is out of bound {}",
                child_index,
                self.children.len()
            ))
        })?;
        memo.group(*group_index)
    }

    pub fn apply_rule(
        &self,
        memo: &Memo,
        rule: &RulePtr,
        transform_state: &mut TransformResult,
    ) -> Result<()> {
        if self.applied_rules.get(&rule.id()) {
            return Ok(());
        }

        let mut extractor = PatternExtractor::new();
        for pattern in rule.matchers() {
            let exprs = extractor.extract(memo, self, pattern)?;
            for expr in exprs.iter() {
                rule.apply(expr, transform_state)?;
            }
            if !exprs.is_empty() {
                break;
            }
        }

        Ok(())
    }
}
