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

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::filter::FilterExecutor;
use databend_common_expression::filter::SelectExpr;
use databend_common_expression::filter::SelectExprBuilder;
use databend_common_expression::filter::generate_like_pattern;
use databend_common_expression::type_check;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NarrowBuffer;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;

/// The LIKE selector matches dictionary values without decoding, including under AND/OR.
/// Nullable dictionaries apply validity after matching so NOT LIKE still rejects NULL.
#[test]
fn test_select_like_over_dictionary() {
    let keys = [1u64, 0, 1, 2, 3, 4, 0, 2];
    let num_rows = keys.len();
    let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
        NarrowBuffer::pack(keys.into_iter(), 5),
        StringType::from_data(vec!["yyy/b", "xxx/a", "xxx/c", "", "xxx_100%", "unused"]),
    ))));
    let func_ctx = FunctionContext::default();
    for nullable in [false, true] {
        let url = if nullable {
            NullableColumn::new_column(
                encoded.clone(),
                Bitmap::from_iter([true, true, false, true, false, true, true, true]),
            )
        } else {
            encoded.clone()
        };
        let columns = [
            ("url", url.data_type()),
            ("f", DataType::Boolean),
            ("k", DataType::Number(NumberDataType::Int32)),
        ];
        let block = DataBlock::new(
            vec![
                url.into(),
                BooleanType::from_data(vec![true, true, false, false, true, true, false, true])
                    .into(),
                Int32Type::from_data((0..num_rows as i32).collect()).into(),
            ],
            num_rows,
        );
        let plain = DataBlock::new(
            vec![
                BlockEntry::try_from(block.get_by_offset(0).decoded_value()).unwrap(),
                block.get_by_offset(1).clone(),
                block.get_by_offset(2).clone(),
            ],
            num_rows,
        );
        let column_ref = type_check::check(
            &parse_raw_expr("url", &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let pattern = generate_like_pattern(b"xxx%".as_slice(), 0);
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
        // Assert that encoded input reaches the fast path, not just that decoding gives
        // correct results. Ordinary strings must instead use the fallback.
        assert!(
            evaluator
                .try_like_encoded(&column_ref, &pattern, false, None)
                .is_some()
        );
        assert!(
            Evaluator::new(&plain, &func_ctx, &BUILTIN_FUNCTIONS)
                .try_like_encoded(&column_ref, &pattern, false, None)
                .is_none()
        );

        for text in [
            "url LIKE 'xxx%'",
            "NOT (url LIKE 'xxx%')",
            "url LIKE '%'",
            "NOT (url LIKE '%')",
            r#""like"(url, 'xxx!_%', '!')"#,
            "f AND url LIKE 'xxx%'",
            "url LIKE 'xxx%' AND f",
            "f OR url LIKE 'xxx%'",
            "url LIKE 'xxx%' OR f",
            "(url LIKE 'xxx%' OR f) AND (NOT (url LIKE '%/a') OR k = 2)",
            "(f AND url LIKE 'xxx%') OR (k > 3 AND NOT (url LIKE '%/a'))",
        ] {
            let expr = type_check::check(
                &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
                &BUILTIN_FUNCTIONS,
            )
            .unwrap();
            let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
                .0
                .into_owned();
            let truth = type_check::check(
                &parse_raw_expr(&format!("is_true({text})"), &columns, &BUILTIN_FUNCTIONS),
                &BUILTIN_FUNCTIONS,
            )
            .unwrap();
            let expected = Evaluator::new(&plain, &func_ctx, &BUILTIN_FUNCTIONS)
                .run(&truth)
                .unwrap()
                .try_downcast::<BooleanType>()
                .unwrap();
            let expected = plain.clone().filter_boolean_value(&expected).unwrap();
            let (select_expr, _): (SelectExpr, bool) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
                .build(&expr)
                .into();
            // LIKE '%' can be folded to a constant / validity check before selection.
            if !text.contains("'%'") {
                assert!(format!("{select_expr:?}").contains("Like(("), "{text}");
            }
            let mut executor = FilterExecutor::new(
                expr,
                func_ctx.clone(),
                num_rows,
                None,
                &BUILTIN_FUNCTIONS,
                true,
            );
            for _ in 0..16 {
                let actual = executor.filter(block.clone()).unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{text}");
                if actual.is_empty() {
                    continue;
                }
                assert_eq!(
                    actual.get_by_offset(2).value(),
                    expected.get_by_offset(2).value(),
                    "{text}"
                );
                assert_eq!(
                    actual.get_by_offset(0).decoded_value(),
                    expected.get_by_offset(0).decoded_value(),
                    "{text}"
                );
                let column = actual.get_by_offset(0).to_column().remove_nullable();
                assert!(column.is_encoded(), "{text}");
            }
        }
    }
}

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

/// The selector take copies runs of selected rows in bulk; with OR the
/// selection may be out of row order, which `keep_order` sorts.
///
/// The block has a variable-length column and a nullable column, so that on
/// runs of 32 rows or more every column takes the runs path
/// (`take_with_optimize_size_by_runs`); random rows keep the row gather.
#[test]
fn test_selector_take_runs() {
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::Int64Type;

    let num_rows = 4096;
    let columns = [
        ("k", DataType::Number(NumberDataType::Int32)),
        ("s", DataType::String),
        ("b", DataType::Binary),
        (
            "n",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
        ),
    ];
    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data((0..num_rows as i32).collect::<Vec<_>>()),
        StringType::from_data(
            (0..num_rows)
                .map(|row| format!("a string longer than a view: {row}"))
                .collect::<Vec<_>>(),
        ),
        BinaryType::from_data(
            (0..num_rows)
                .map(|row| format!("bytes {row}").into_bytes())
                .collect::<Vec<_>>(),
        ),
        Int64Type::from_data_with_validity(
            (0..num_rows as i64).collect::<Vec<_>>(),
            (0..num_rows).map(|row| row % 5 != 0).collect::<Vec<_>>(),
        ),
    ]);
    let func_ctx = FunctionContext::default();
    let mut unordered_selections = 0;
    // `runs`: the selection has runs of 32 rows or more on average, the
    // largest threshold of `take_with_optimize_size_by_runs`.
    for (text, runs) in [
        // Runs of 128 rows, about 25% selected.
        ("k % 512 < 128", true),
        // OR appends the rows of the second branch after those of the first.
        ("k >= 3000 OR k % 512 < 128", true),
        // Random rows.
        ("k * 7919 % 13 < 5", false),
        ("k * 7919 % 13 < 3 OR k * 104729 % 11 < 3", false),
        // More than 80% selected: the range take.
        ("k >= 3000 OR k % 512 < 450", true),
    ] {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let truth = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
            .run(&expr)
            .unwrap()
            .try_downcast::<BooleanType>()
            .unwrap();
        let expected = block.clone().filter_boolean_value(&truth).unwrap();
        for keep_order in [true, false] {
            let mut executor = FilterExecutor::new(
                expr.clone(),
                func_ctx.clone(),
                num_rows,
                None,
                &BUILTIN_FUNCTIONS,
                keep_order,
            );
            let count = executor.select(&block).unwrap();
            let selection = executor.true_selection()[..count].to_vec();
            let selection_runs = 1 + selection.windows(2).filter(|w| w[0] + 1 != w[1]).count();
            assert_eq!(selection.len() >= 32 * selection_runs, runs, "{text}");
            let by_runs = block.take_with_optimize_size_by_runs(&selection).unwrap();
            let by_rows = block.take_with_optimize_size(selection.as_slice()).unwrap();
            for (l, r) in by_runs.columns().iter().zip(by_rows.columns()) {
                assert_eq!(l.value(), r.value(), "{text}");
            }
            let actual = executor.take(block.clone(), num_rows, count).unwrap();
            assert_eq!(actual.num_rows(), expected.num_rows(), "{text}");
            if keep_order {
                for (l, r) in actual.columns().iter().zip(expected.columns()) {
                    assert_eq!(l.value(), r.value(), "{text}");
                }
            } else {
                // The rows come in the order of the selection.
                let reference = block.take(selection.as_slice()).unwrap();
                unordered_selections += !selection.is_sorted() as usize;
                let mut sorted = selection.clone();
                sorted.sort();
                assert!(sorted.windows(2).all(|w| w[0] < w[1]), "{text}");
                assert_eq!(sorted.len(), expected.num_rows(), "{text}");
                if actual.num_rows() * 10 > num_rows * 8 {
                    // The range take sorts the selection first.
                    let expected_sorted = block.take(sorted.as_slice()).unwrap();
                    for (l, r) in actual.columns().iter().zip(expected_sorted.columns()) {
                        assert_eq!(l.value(), r.value(), "{text}");
                    }
                } else {
                    for (l, r) in actual.columns().iter().zip(reference.columns()) {
                        assert_eq!(l.value(), r.value(), "{text}");
                    }
                }
            }
        }
    }
    assert!(unordered_selections > 0);
}

/// Fuse prewhere switches to the selector executor only for predicates that contain a
/// `<string column> = <string constant>` comparison, at any depth of AND/OR.
#[test]
fn test_contains_string_column_eq_constant() {
    let columns = [
        ("s", DataType::String),
        ("t", DataType::String),
        ("ns", DataType::Nullable(Box::new(DataType::String))),
        ("k", DataType::Number(NumberDataType::Int32)),
    ];
    let func_ctx = FunctionContext::default();
    for (text, expected) in [
        ("s = 'v'", true),
        ("'v' = s", true),
        // Folding turns the cast of the literal into a constant, as the planner does.
        ("ns = 'v'", true),
        ("'v' = ns", true),
        ("k > 1 AND s = 'v'", true),
        ("k > 1 OR (k < 0 AND ns = 'v')", true),
        ("s <> 'v'", false),
        ("s = t", false),
        ("k = 1", false),
        ("s LIKE 'v%'", false),
        ("k > 1 OR s > 'v'", false),
    ] {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned();
        let (select_expr, _): (SelectExpr, bool) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
            .build(&expr)
            .into();
        assert_eq!(
            select_expr.contains_string_column_eq_constant(),
            expected,
            "{text}: {select_expr:?}"
        );
    }
}

/// `and` / `or` over a nullable and a non-nullable predicate wrap the non-nullable one in
/// `CAST(.. AS Boolean NULL)`. The selector looks through that cast, also under NOT, and must
/// select the same rows as the evaluator, including rows where the nullable column is NULL.
#[test]
fn test_select_mixed_nullability_and_or_not() {
    let num_rows = 8;
    let block = DataBlock::new_from_columns(vec![
        StringType::from_opt_data(vec![
            Some("a"),
            None,
            Some("b"),
            Some("a"),
            None,
            Some("b"),
            Some("a"),
            None,
        ]),
        StringType::from_data(vec!["x", "x", "y", "y", "x", "y", "x", "y"]),
        Int32Type::from_data((0..num_rows as i32).collect()),
    ]);
    let columns = [
        ("ns", DataType::Nullable(Box::new(DataType::String))),
        ("s", DataType::String),
        ("k", DataType::Number(NumberDataType::Int32)),
    ];
    let func_ctx = FunctionContext::default();

    for text in [
        "ns = 'a' OR k < 5",
        "'a' = ns OR k < 2",
        "s = 'x' AND (ns = 'a' OR k < 5)",
        "(ns = 'b' AND k > 1) OR s = 'x'",
        "NOT (ns = 'a' OR k < 5)",
        "NOT (ns = 'a' AND k > 2)",
        "NOT (ns = 'a') OR k > 5",
        "s = 'y' AND NOT (ns <> 'b' OR k = 2)",
    ] {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned();
        let (select_expr, _): (SelectExpr, bool) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
            .build(&expr)
            .into();
        let debug = format!("{select_expr:?}");
        assert!(!debug.contains("Others"), "{text}: {debug}");

        let truth = type_check::check(
            &parse_raw_expr(&format!("is_true({text})"), &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expected = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
            .run(&truth)
            .unwrap()
            .try_downcast::<BooleanType>()
            .unwrap();
        let expected = block.clone().filter_boolean_value(&expected).unwrap();

        let mut executor = FilterExecutor::new(
            expr,
            func_ctx.clone(),
            num_rows,
            None,
            &BUILTIN_FUNCTIONS,
            true,
        );
        for _ in 0..8 {
            let actual = executor.filter(block.clone()).unwrap();
            assert_eq!(actual.num_rows(), expected.num_rows(), "{text}");
            assert_eq!(
                actual.get_by_offset(2).value(),
                expected.get_by_offset(2).value(),
                "{text}"
            );
        }
    }
}

/// `is_true` turns NULL into false, so a negation must not be pushed through it, including
/// when `is_true` sits under a nullable boolean cast added by `or`. The selector must select
/// the same rows as the evaluator.
#[test]
fn test_select_negated_is_true() {
    let num_rows = 6;
    let block = DataBlock::new_from_columns(vec![
        StringType::from_opt_data(vec![Some("x"), None, Some("y"), None, Some("x"), Some("z")]),
        StringType::from_opt_data(vec![Some("y"), Some("z"), None, Some("y"), Some("z"), None]),
        StringType::from_data(vec!["a_1", "ab", "a_2", "a!_", "b_", "a_"]),
        StringType::from_data(vec!["!", "!", "#", "!", "!", "!"]),
        Int32Type::from_data((0..num_rows as i32).collect()),
    ]);
    let columns = [
        ("ns", DataType::Nullable(Box::new(DataType::String))),
        ("ns2", DataType::Nullable(Box::new(DataType::String))),
        ("v", DataType::String),
        ("e", DataType::String),
        ("k", DataType::Number(NumberDataType::Int32)),
    ];
    let func_ctx = FunctionContext::default();

    for text in [
        "NOT is_true(ns <> 'x')",
        "NOT (is_true(ns <> 'x') OR ns2 = 'y')",
        "k > 0 AND NOT (is_true(ns <> 'x') AND ns2 = 'z')",
        r#""like"(v, 'a!_%', '!')"#,
        r#"NOT "like"(v, 'a!_%', '!')"#,
    ] {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned();
        let truth = type_check::check(
            &parse_raw_expr(&format!("is_true({text})"), &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expected = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
            .run(&truth)
            .unwrap()
            .try_downcast::<BooleanType>()
            .unwrap();
        let expected = block.clone().filter_boolean_value(&expected).unwrap();

        let mut executor = FilterExecutor::new(
            expr,
            func_ctx.clone(),
            num_rows,
            None,
            &BUILTIN_FUNCTIONS,
            true,
        );
        for _ in 0..8 {
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

/// The selector compiles only a constant LIKE escape into the pattern. An escape read from a
/// column is left to the evaluator instead of being ignored.
#[test]
fn test_select_like_with_column_escape_is_not_compiled() {
    let columns = [("v", DataType::String), ("e", DataType::String)];
    let func_ctx = FunctionContext::default();
    for (text, compiled) in [
        (r#""like"(v, 'a!_%', '!')"#, true),
        (r#""like"(v, 'a!_%', e)"#, false),
        (r#"NOT "like"(v, 'a!_%', e)"#, false),
    ] {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned();
        let (select_expr, _): (SelectExpr, bool) = SelectExprBuilder::new(&BUILTIN_FUNCTIONS)
            .build(&expr)
            .into();
        assert_eq!(
            matches!(select_expr, SelectExpr::Like(_)),
            compiled,
            "{text}: {select_expr:?}"
        );
        if !compiled {
            assert!(matches!(select_expr, SelectExpr::Others(_)), "{text}");
        }
    }
}

/// With a nullable `ns`, `ns = 'keep' AND ..` gets `CAST(.. AS Boolean NULL)` around its
/// non-nullable side. The selector looks through that cast but must not reorder across blocks,
/// neither the conjuncts around the cast nor the AND/OR inside it: `parse_json` would then run
/// on rows a guard rejected and raise their errors.
#[test]
fn test_select_nullable_boolean_cast_keeps_conjunct_order() {
    let num_rows = 6;
    let block = DataBlock::new_from_columns(vec![
        StringType::from_opt_data(vec![
            Some("keep"),
            Some("keep"),
            Some("no"),
            Some("keep"),
            Some("keep"),
            None,
        ]),
        StringType::from_data(vec!["ok", "no", "ok", "ok", "no", "no"]),
        StringType::from_data(vec!["invalid", "{}", "invalid", "invalid", "[]", "invalid"]),
        Int32Type::from_data((0..num_rows as i32).collect()),
    ]);
    let columns = [
        ("ns", DataType::Nullable(Box::new(DataType::String))),
        ("s", DataType::String),
        ("v", DataType::String),
        ("k", DataType::Number(NumberDataType::Int32)),
    ];
    let func_ctx = FunctionContext::default();
    for (text, expected) in [
        // The guard is next to the cast.
        (
            "ns = 'keep' AND s = 'no' AND parse_json(v) = parse_json('{}')",
            vec![1],
        ),
        // The guard is inside the cast.
        (
            "ns = 'keep' AND (s = 'ok' OR parse_json(v) = parse_json('{}'))",
            vec![0, 1, 3],
        ),
    ] {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        let expr = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned();
        assert!(
            expr.sql_display().contains("CAST"),
            "{}",
            expr.sql_display()
        );

        let mut executor = FilterExecutor::new(
            expr,
            func_ctx.clone(),
            num_rows,
            None,
            &BUILTIN_FUNCTIONS,
            true,
        );
        // Adaptive reordering would swap two reorderable children after the first block.
        for _ in 0..16 {
            let actual = executor.filter(block.clone()).unwrap();
            assert_eq!(
                actual.get_by_offset(3).to_column(),
                Int32Type::from_data(expected.clone()),
                "{text}"
            );
        }
    }
}
