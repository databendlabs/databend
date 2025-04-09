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
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::RelOp;
use crate::plans::Sort;

pub struct RuleEliminateSort {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleEliminateSort {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateSort,
            // Sort
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Sort,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleEliminateSort {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let input = s_expr.child(0)?;

        let rel_expr = RelExpr::with_s_expr(input);
        let prop = rel_expr.derive_relational_prop()?;

        if let Some(window) = &sort.window_partition {
            if let Some((partition, ordering)) = &prop.partition_orderings {
                // must has same partition
                // if the ordering of the current node is empty, we can eliminate the sort
                // eg: explain  select number, sum(number - 1) over (partition by number % 3 order by number + 1),
                // avg(number) over (partition by number % 3 order by number + 1)
                // from numbers(50);
                if partition == &window.partition_by
                    && (ordering == &sort.items || sort.sort_items_exclude_partition().is_empty())
                {
                    state.add_result(input.clone());
                    return Ok(());
                }
            }
        }

        if prop.orderings == sort.items {
            // If the derived ordering is completely equal to
            // the sort operator, we can eliminate the sort.
            state.add_result(input.clone());
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleEliminateSort {
    fn default() -> Self {
        Self::new()
    }
}
