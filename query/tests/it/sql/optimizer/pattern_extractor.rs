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

use databend_query::sql::optimizer::MExpr;
use databend_query::sql::optimizer::Memo;
use databend_query::sql::optimizer::PatternExtractor;
use databend_query::sql::optimizer::SExpr;
use databend_query::sql::LogicalGet;
use databend_query::sql::LogicalProject;
use databend_query::sql::Plan;

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
    memo.init(expr).unwrap();

    memo.insert_m_expr(
        0,
        MExpr::create(0, Rc::new(Plan::LogicalGet(LogicalGet::default())), vec![]),
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
        Rc::new(Plan::LogicalProject(LogicalProject::default())),
        vec![SExpr::create(
            Rc::new(Plan::LogicalGet(LogicalGet::default())),
            vec![],
            Some(0),
        )],
        Some(1),
    );

    let expected = vec![expected_expr.clone(), expected_expr];
    assert_eq!(result, expected);
}
