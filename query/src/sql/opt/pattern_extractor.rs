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

use crate::sql::opt::group::Group;
use crate::sql::opt::m_expr::MExpr;
use crate::sql::opt::memo::Memo;
use crate::sql::opt::SExpr;

/// A helper to extract `SExpr`s from `Memo` that match the given pattern.
pub struct PatternExtractor {}

impl PatternExtractor {
    pub fn create() -> Self {
        PatternExtractor {}
    }

    pub fn extract(&mut self, memo: &Memo, m_expr: &MExpr, pattern: &SExpr) -> Vec<SExpr> {
        if !m_expr.match_pattern(memo, pattern) {
            return vec![];
        }

        if pattern.is_pattern() {
            // Pattern operator is `Pattern`, we can return current operator.
            return vec![SExpr::create(
                m_expr.plan(),
                vec![],
                Some(m_expr.group_index()),
            )];
        }

        let pattern_children = pattern.children();

        if m_expr.arity() != pattern_children.len() {
            return vec![];
        }

        let mut children_results = vec![];
        for (i, child) in m_expr.children().iter().enumerate().take(m_expr.arity()) {
            let pattern = &pattern_children[i];
            let child_group = memo.group(*child);
            let result = self.extract_group(memo, child_group, pattern);
            children_results.push(result);
        }

        Self::generate_expression_with_children(m_expr, children_results)
    }

    fn extract_group(&mut self, memo: &Memo, group: &Group, pattern: &SExpr) -> Vec<SExpr> {
        let mut results = vec![];
        for group_expression in group.iter() {
            let mut result = self.extract(memo, group_expression, pattern);
            results.append(&mut result);
        }

        results
    }

    fn generate_expression_with_children(
        m_expr: &MExpr,
        candidates: Vec<Vec<SExpr>>,
    ) -> Vec<SExpr> {
        let mut results = vec![];

        // Initialize cursors
        let mut cursors: Vec<usize> = vec![];
        for candidate in candidates.iter() {
            if candidate.is_empty() {
                // Every child should have at least one candidate
                return results;
            }
            cursors.push(0);
        }

        if cursors.is_empty() {
            results.push(SExpr::create(
                m_expr.plan(),
                vec![],
                Some(m_expr.group_index()),
            ));
            return results;
        }

        'LOOP: loop {
            let mut children: Vec<SExpr> = vec![];
            for (index, cursor) in cursors.iter().enumerate() {
                children.push(candidates[index][*cursor].clone());
            }
            results.push(SExpr::create(
                m_expr.plan().clone(),
                children,
                Some(m_expr.group_index()),
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

        results
    }
}

#[cfg(test)]
mod test {
    use std::rc::Rc;

    use crate::sql::opt::m_expr::MExpr;
    use crate::sql::opt::memo::Memo;
    use crate::sql::opt::pattern_extractor::PatternExtractor;
    use crate::sql::opt::SExpr;
    use crate::sql::LogicalGet;
    use crate::sql::LogicalProject;
    use crate::sql::Plan;

    #[test]
    fn test_unary_expression() {
        // LogicalProject
        // \
        //  LogicalGet
        let expr = SExpr::create_unary(
            Rc::new(Plan::LogicalProject(LogicalProject::default())),
            SExpr::create_leaf(Rc::new(Plan::LogicalGet(LogicalGet::default()))),
        );

        // LogicalProject
        // \
        //  Pattern
        let pattern = SExpr::create_unary(
            Rc::new(Plan::LogicalProject(Default::default())),
            SExpr::create_leaf(Rc::new(Plan::Pattern)),
        );

        let mut pattern_extractor = PatternExtractor::create();
        let mut memo = Memo::create();
        memo.init(expr.clone()).unwrap();

        let group_expression = memo
            .root()
            .unwrap()
            .iter()
            .take(1)
            .cloned()
            .collect::<Vec<MExpr>>()[0]
            .clone();
        let result = pattern_extractor.extract(&memo, &group_expression, &pattern);

        let expected = vec![SExpr::create(
            Rc::new(Plan::LogicalProject(LogicalProject::default())),
            vec![SExpr::create(
                Rc::new(Plan::LogicalGet(LogicalGet::default())),
                vec![],
                Some(0),
            )],
            Some(1),
        )];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_multiple_expression() {
        // LogicalProject
        // \
        //  LogicalGet
        let expr = SExpr::create_unary(
            Rc::new(Plan::LogicalProject(LogicalProject::default())),
            SExpr::create_leaf(Rc::new(Plan::LogicalGet(LogicalGet::default()))),
        );

        // LogicalProject
        // \
        //  LogicalGet
        let pattern = SExpr::create_unary(
            Rc::new(Plan::LogicalProject(Default::default())),
            SExpr::create_leaf(Rc::new(Plan::LogicalGet(LogicalGet::default()))),
        );

        let mut pattern_extractor = PatternExtractor::create();
        let mut memo = Memo::create();
        memo.init(expr.clone()).unwrap();

        memo.insert_m_expr(
            0,
            MExpr::create(0, Rc::new(Plan::LogicalGet(LogicalGet::default())), vec![]),
        )
        .unwrap();

        println!("{:?}", memo);

        let group_expression = memo
            .root()
            .unwrap()
            .iter()
            .take(1)
            .cloned()
            .collect::<Vec<MExpr>>()[0]
            .clone();
        let result = pattern_extractor.extract(&memo, &group_expression, &pattern);

        let expected_expr = SExpr::create(
            Rc::new(Plan::LogicalProject(LogicalProject::default())),
            vec![SExpr::create(
                Rc::new(Plan::LogicalGet(LogicalGet::default())),
                vec![],
                Some(0),
            )],
            Some(1),
        );

        let expected = vec![expected_expr.clone(), expected_expr.clone()];
        assert_eq!(result, expected);
    }
}
