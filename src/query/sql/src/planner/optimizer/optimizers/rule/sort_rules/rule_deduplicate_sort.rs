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
use crate::plans::RelOp;
use crate::plans::Sort;

/// Rule to remove duplicate sort items in ORDER BY clause.
/// 
/// For example, transforms:
/// ORDER BY x ASC NULLS LAST, x ASC NULLS LAST
/// into:
/// ORDER BY x ASC NULLS LAST
/// 
/// This optimization is valid because duplicate sort fields don't contribute
/// additional ordering and only add unnecessary computational overhead.
pub struct RuleDeduplicateSort {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleDeduplicateSort {
    pub fn new() -> Self {
        Self {
            id: RuleID::DeduplicateSort,
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

impl Rule for RuleDeduplicateSort {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;
        
        if sort.items.len() <= 1 {
            return Ok(());
        }
        
        // Deduplicate sort items while preserving order
        let mut deduplicated_items = Vec::with_capacity(sort.items.len());
        let mut seen = std::collections::HashSet::with_capacity(sort.items.len());
        
        for item in &sort.items {
            if seen.insert(item.clone()) {
                deduplicated_items.push(item.clone());
            }
        }
        
        // Only apply transformation if we actually removed duplicates
        if deduplicated_items.len() == sort.items.len() {
            return Ok(());
        }

        let new_sort = Sort {
            items: deduplicated_items,
            limit: sort.limit,
            after_exchange: sort.after_exchange,
            pre_projection: sort.pre_projection,
            window_partition: sort.window_partition,
        };
        
        let mut result = s_expr.replace_plan(std::sync::Arc::new(new_sort.into()));
        result.set_applied_rule(&self.id);
        state.add_result(result);
        
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleDeduplicateSort {
    fn default() -> Self {
        Self::new()
    }
}
