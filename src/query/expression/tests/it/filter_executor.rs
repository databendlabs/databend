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

use std::borrow::Cow;

use databend_common_expression::BlockEntry;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::filter::FilterExecutor;
use databend_common_expression::filter::SelectExpr;
use databend_common_expression::filter::SelectExprBuilder;
use databend_common_expression::type_check;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;

/// The null-extended side of an outer join is a constant NULL column. A LIKE over it
/// selects nothing, but it must still hand its rows on as false so that an enclosing OR
/// evaluates its next child over them.
#[test]
fn test_select_like_over_constant_null() {
    let num_rows = 8;
    let block = DataBlock::new(
        vec![
            BlockEntry::new_const_column(
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
                Scalar::Null,
                num_rows,
            ),
            BlockEntry::new_const_column(
                DataType::Nullable(Box::new(DataType::String)),
                Scalar::Null,
                num_rows,
            ),
            BooleanType::from_data(vec![true, true, false, false, true, true, true, true]).into(),
            Int32Type::from_data(vec![1, 0, 1, 0, 1, 1, 0, 1]).into(),
            // Row id, to compare which rows were selected.
            Int32Type::from_data((0..num_rows as i32).collect()).into(),
        ],
        num_rows,
    );
    let columns = [
        (
            "x",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
        ),
        ("c", DataType::Nullable(Box::new(DataType::String))),
        ("f", DataType::Boolean),
        ("d", DataType::Number(NumberDataType::Int32)),
        ("k", DataType::Number(NumberDataType::Int32)),
    ];

    for text in [
        "(x = 10 OR f) AND (c LIKE '_' OR d = 1)",
        "(c LIKE '_' OR f) AND (c LIKE '_' OR d = 1)",
        "f AND (c LIKE '_' OR d = 1)",
        "(c LIKE '_' OR d = 1) AND f",
        "(NOT (c LIKE 'a%') OR f) AND (NOT (c LIKE 'a%') OR d = 1)",
        "c LIKE '_' OR f",
        "f AND c LIKE '_'",
    ] {
        let func_ctx = FunctionContext::default();
        let raw_expr = parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS);
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        // Fold the casts of the literals as the planner does, so that LIKE compiles to the
        // selector's LIKE fast path rather than a generic function call.
        let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned();

        // A NULL predicate drops the row, as in a WHERE clause.
        let raw_truth = parse_raw_expr(&format!("is_true({text})"), &columns, &BUILTIN_FUNCTIONS);
        let truth = type_check::check(&raw_truth, &BUILTIN_FUNCTIONS).unwrap();
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
        let expected = evaluator
            .run(&truth)
            .unwrap()
            .try_downcast::<BooleanType>()
            .unwrap();
        let expected = block.clone().filter_boolean_value(&expected).unwrap();

        let (select_expr, _): (SelectExpr, bool) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
            .build(&expr)
            .into();
        assert!(
            format!("{select_expr:?}").contains("Like(("),
            "{text} must use the LIKE fast path"
        );

        let mut executor =
            FilterExecutor::new(expr, func_ctx, num_rows, None, &BUILTIN_FUNCTIONS, true);
        // The selector reorders AND/OR children adaptively; repeat to cover several orders.
        for _ in 0..16 {
            let actual = executor.filter(block.clone()).unwrap();
            assert_eq!(actual.num_rows(), expected.num_rows(), "{text}");
            assert_eq!(
                actual.get_by_offset(4).value(),
                expected.get_by_offset(4).value(),
                "{text}"
            );
        }
    }
}
