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

use databend_query::sql::optimizer::MExpr;
use databend_query::sql::optimizer::Memo;
use databend_query::sql::optimizer::PatternExtractor;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::plans::Operator;
use databend_query::sql::plans::PatternPlan;
use databend_query::sql::plans::RelOp;

fn compare_s_expr(lhs: &SExpr, rhs: &SExpr) -> bool {
    // Compare children first
    if lhs.arity() != rhs.arity() {
        return false;
    }

    for (l_child, r_child) in lhs.children().iter().zip(rhs.children().iter()) {
        if !compare_s_expr(l_child, r_child) {
            return false;
        }
    }

    lhs.plan().rel_op() == rhs.plan().rel_op()
}

#[test]
fn test_unary_expression() {
    // Project
    // \
    //  LogicalGet
    let expr = SExpr::create_unary(
        PatternPlan {
            plan_type: RelOp::Project,
        }
        .into(),
        SExpr::create_leaf(
            PatternPlan {
                plan_type: RelOp::LogicalGet,
            }
            .into(),
        ),
    );

    // Project
    // \
    //  Pattern
    let pattern = SExpr::create_unary(
        From::from(PatternPlan {
            plan_type: RelOp::Project,
        }),
        SExpr::create_leaf(From::from(PatternPlan {
            plan_type: RelOp::Pattern,
        })),
    );

    let mut pattern_extractor = PatternExtractor::create();
    let mut memo = Memo::create();
    memo.init(expr).unwrap();

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
        From::from(PatternPlan {
            plan_type: RelOp::Project,
        }),
        vec![SExpr::create(
            From::from(PatternPlan {
                plan_type: RelOp::LogicalGet,
            }),
            vec![],
            Some(0),
        )],
        Some(1),
    )];
    assert!(compare_s_expr(&result[0], &expected[0]));
}

#[test]
fn test_multiple_expression() {
    // Project
    // \
    //  LogicalGet
    let expr = SExpr::create_unary(
        From::from(PatternPlan {
            plan_type: RelOp::Project,
        }),
        SExpr::create_leaf(From::from(PatternPlan {
            plan_type: RelOp::LogicalGet,
        })),
    );

    // Project
    // \
    //  LogicalGet
    let pattern = SExpr::create_unary(
        PatternPlan {
            plan_type: RelOp::Project,
        }
        .into(),
        SExpr::create_leaf(
            PatternPlan {
                plan_type: RelOp::LogicalGet,
            }
            .into(),
        ),
    );

    let mut pattern_extractor = PatternExtractor::create();
    let mut memo = Memo::create();
    memo.init(expr).unwrap();

    memo.insert_m_expr(
        0,
        MExpr::create(
            0,
            PatternPlan {
                plan_type: RelOp::LogicalGet,
            }
            .into(),
            vec![],
        ),
    )
    .unwrap();

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
        PatternPlan {
            plan_type: RelOp::Project,
        }
        .into(),
        vec![SExpr::create(
            PatternPlan {
                plan_type: RelOp::LogicalGet,
            }
            .into(),
            vec![],
            Some(0),
        )],
        Some(1),
    );

    let expected = vec![expected_expr.clone(), expected_expr];
    assert!(compare_s_expr(&result[0], &expected[0]));
}
