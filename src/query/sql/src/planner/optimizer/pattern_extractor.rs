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

use crate::optimizer::group::Group;
use crate::optimizer::m_expr::MExpr;
use crate::optimizer::memo::Memo;
use crate::optimizer::SExpr;

/// A helper to extract `SExpr`s from `Memo` that match the given pattern.
pub struct PatternExtractor {}

impl PatternExtractor {
    pub fn create() -> Self {
        PatternExtractor {}
    }

    pub fn extract(&mut self, memo: &Memo, m_expr: &MExpr, pattern: &SExpr) -> Result<Vec<SExpr>> {
        if !m_expr.match_pattern(memo, pattern) {
            return Ok(vec![]);
        }

        if pattern.is_pattern() {
            // Expand the pattern node to a complete `SExpr`.
            let child = Self::expand_pattern(memo, m_expr)?;
            return Ok(vec![child]);
        }

        let pattern_children = pattern.children();

        if m_expr.arity() != pattern_children.len() {
            return Ok(vec![]);
        }

        let mut children_results = vec![];
        for (i, child) in m_expr.children.iter().enumerate().take(m_expr.arity()) {
            let pattern = &pattern_children[i];
            let child_group = memo.group(*child)?;
            let result = self.extract_group(memo, child_group, pattern)?;
            children_results.push(result);
        }

        Self::generate_expression_with_children(memo, m_expr, children_results)
    }

    fn extract_group(&mut self, memo: &Memo, group: &Group, pattern: &SExpr) -> Result<Vec<SExpr>> {
        let mut results = vec![];
        for m_expr in group.m_exprs.iter() {
            let result = self.extract(memo, m_expr, pattern)?;
            results.extend(result.into_iter());
        }

        Ok(results)
    }

    fn generate_expression_with_children(
        memo: &Memo,
        m_expr: &MExpr,
        candidates: Vec<Vec<SExpr>>,
    ) -> Result<Vec<SExpr>> {
        let mut results = vec![];

        // Initialize cursors
        let mut cursors: Vec<usize> = vec![];
        for candidate in candidates.iter() {
            if candidate.is_empty() {
                // Every child should have at least one candidate
                return Ok(results);
            }
            cursors.push(0);
        }

        if cursors.is_empty() {
            results.push(SExpr::create(
                m_expr.plan.clone(),
                vec![],
                Some(m_expr.group_index),
                Some(memo.group(m_expr.group_index)?.relational_prop.clone()),
                Some(memo.group(m_expr.group_index)?.stat_info.clone()),
            ));
            return Ok(results);
        }

        'LOOP: loop {
            let mut children = vec![];
            for (index, cursor) in cursors.iter().enumerate() {
                children.push(Arc::new(candidates[index][*cursor].clone()));
            }
            results.push(SExpr::create(
                m_expr.plan.clone(),
                children,
                Some(m_expr.group_index),
                Some(memo.group(m_expr.group_index)?.relational_prop.clone()),
                Some(memo.group(m_expr.group_index)?.stat_info.clone()),
            ));

            let mut shifted = false;
            // Shift cursor
            for i in (0..cursors.len()).rev() {
                if !shifted {
                    // Shift cursor
                    cursors[i] += 1;
                    shifted = true;
                }

                if i == 0 && cursors[0] > candidates[0].len() - 1 {
                    // Candidates are exhausted
                    break 'LOOP;
                } else if i > 0 && cursors[i] > candidates[i].len() - 1 {
                    // Shift previous children
                    cursors[i] = 0;
                    cursors[i - 1] += 1;
                    continue;
                } else {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Expand a `Pattern` node to an arbitrary `SExpr` with `m_expr` as the root.
    /// Since we don't care about the actual content of the `Pattern` node, we will
    /// choose the first `MExpr` in each group to construct the `SExpr`.
    fn expand_pattern(memo: &Memo, m_expr: &MExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(m_expr.arity());
        for child in m_expr.children.iter() {
            let child_group = memo.group(*child)?;
            let child_m_expr = child_group
                .m_exprs
                .first()
                .ok_or_else(|| ErrorCode::Internal(format!("No MExpr in group {child}")))?;
            children.push(Arc::new(Self::expand_pattern(memo, child_m_expr)?));
        }

        Ok(SExpr::create(
            m_expr.plan.clone(),
            children,
            Some(m_expr.group_index),
            Some(memo.group(m_expr.group_index)?.relational_prop.clone()),
            Some(memo.group(m_expr.group_index)?.stat_info.clone()),
        ))
    }
}
