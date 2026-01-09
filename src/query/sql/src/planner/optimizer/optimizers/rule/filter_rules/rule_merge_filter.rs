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

use databend_common_exception::Result;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::RelOp;

// Merge two adjacent `Filter`s into one
pub struct RuleMergeFilter {
    matchers: Vec<Matcher>,
}

impl RuleMergeFilter {
    pub fn new() -> Self {
        Self {
            // Filter
            // \
            //  Filter
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RuleMergeFilter {
    fn id(&self) -> RuleID {
        RuleID::MergeFilter
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let up_filter = s_expr.plan().as_filter().unwrap();
        let down_filter = s_expr.unary_child().plan().as_filter().unwrap();

        let predicates = up_filter
            .predicates
            .iter()
            .chain(down_filter.predicates.iter())
            .cloned()
            .collect();

        state.add_result(
            s_expr
                .unary_child()
                .unary_child_arc()
                .ref_build_unary(Filter { predicates }),
        );
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleMergeFilter {
    fn default() -> Self {
        Self::new()
    }
}
