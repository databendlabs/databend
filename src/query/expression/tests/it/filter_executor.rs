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
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::SelectOp;
use databend_common_expression::Value;
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
            encoded.clone().wrap_nullable(Some(Bitmap::from_iter([
                true, true, false, true, false, true, true, true,
            ])))
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
        let pattern = generate_like_pattern(b"xxx%".as_slice());
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

/// String comparisons in `WHERE` compare the views of the rows. Every operator, with a constant
/// on either side, between two columns, over a nullable column and under AND/OR (which select
/// from the true or the false rows only), must keep the rows that `str` ordering keeps.
#[test]
fn test_select_string_comparisons() {
    const VALUES: [&str; 18] = [
        "",
        "\0",
        "a",
        "a\0",
        "ab",
        "abcd",
        "abcd\0",
        "abce",
        "abcdefghijkl",
        "abcdefghijkm",
        "abcdefghijklm",
        "abcdXfghijklmn",
        "abcdYfghijklmn",
        "\u{e9}",
        "zzzz",
        "https://www.example.com/a",
        "https://www.example.com/b",
        "https://www.example.com/ab",
    ];
    // Constants of the SQL text, which cannot hold `\0`.
    const CONSTANTS: [&str; 8] = [
        "",
        "a",
        "abcd",
        "abcdefghijkl",
        "abcdefghijklm",
        "abcdYfghijklmn",
        "\u{e9}",
        "https://www.example.com/a",
    ];

    let pairs = VALUES
        .iter()
        .flat_map(|s| VALUES.iter().map(move |t| (*s, *t)))
        .collect::<Vec<_>>();
    let num_rows = pairs.len();
    let s = pairs.iter().map(|(s, _)| *s).collect::<Vec<_>>();
    let t = pairs.iter().map(|(_, t)| *t).collect::<Vec<_>>();
    let n = (0..num_rows)
        .map(|row| (row % 5 != 0).then_some(s[row]))
        .collect::<Vec<_>>();
    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data((0..num_rows as i32).collect()),
        StringType::from_data(s.clone()),
        StringType::from_data(t.clone()),
        StringType::from_opt_data(n.clone()),
    ]);
    // The same rows with `s` and `n` dictionary encoded, then also `t`: comparisons with a
    // constant or a column then go through the dictionary instead of decoding it.
    let dict = |column: Column| {
        let (column, validity) = match column {
            Column::Nullable(column) => (column.column().clone(), Some(column.validity().clone())),
            column => (column, None),
        };
        let encoded = DictColumn::try_encode(&column, 1 << 16).unwrap();
        let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(encoded)));
        match validity {
            Some(validity) => encoded.wrap_nullable(Some(validity)),
            None => encoded,
        }
    };
    let encode = |block: &DataBlock, offsets: &[usize]| {
        let columns = (0..block.num_columns())
            .map(|offset| {
                let column = block.get_by_offset(offset).to_column();
                if offsets.contains(&offset) {
                    dict(column)
                } else {
                    column
                }
            })
            .collect();
        DataBlock::new_from_columns(columns)
    };
    let blocks = [
        block.clone(),
        encode(&block, &[1, 3]),
        encode(&block, &[1, 2, 3]),
    ];
    let columns = [
        ("k", DataType::Number(NumberDataType::Int32)),
        ("s", DataType::String),
        ("t", DataType::String),
        ("n", DataType::Nullable(Box::new(DataType::String))),
    ];

    type Cmp = fn(&str, &str) -> bool;
    let ops: [(&str, Cmp); 6] = [
        ("=", |a, b| a == b),
        ("<>", |a, b| a != b),
        ("<", |a, b| a < b),
        ("<=", |a, b| a <= b),
        (">", |a, b| a > b),
        (">=", |a, b| a >= b),
    ];
    // The closures outlive this scope in `cases`.
    let (s, t, n): (&'static [&str], &'static [&str], &'static [Option<&str>]) = (
        Vec::leak(s.clone()),
        Vec::leak(t.clone()),
        Vec::leak(n.clone()),
    );
    // An expression and whether it keeps a row.
    type Case = (String, Box<dyn Fn(usize) -> bool>);
    let mut cases: Vec<Case> = vec![];
    for (op, cmp) in ops {
        cases.push((
            format!("s {op} t"),
            Box::new(move |row| cmp(s[row], t[row])),
        ));
        cases.push((
            format!("n {op} t"),
            Box::new(move |row| n[row].is_some_and(|n| cmp(n, t[row]))),
        ));
        // `k % 3 = 0` selects some rows first, so the comparison sees only the false rows
        // under OR, and `k % 2 = 0` only the true rows under AND.
        cases.push((
            format!("k % 3 = 0 OR s {op} t"),
            Box::new(move |row| row % 3 == 0 || cmp(s[row], t[row])),
        ));
        cases.push((
            format!("k % 3 = 0 OR n {op} t"),
            Box::new(move |row| row % 3 == 0 || n[row].is_some_and(|n| cmp(n, t[row]))),
        ));
        for c in CONSTANTS {
            cases.push((format!("s {op} '{c}'"), Box::new(move |row| cmp(s[row], c))));
            cases.push((format!("'{c}' {op} s"), Box::new(move |row| cmp(c, s[row]))));
            cases.push((
                format!("n {op} '{c}'"),
                Box::new(move |row| n[row].is_some_and(|n| cmp(n, c))),
            ));
            cases.push((
                format!("k % 2 = 0 AND s {op} '{c}'"),
                Box::new(move |row| row % 2 == 0 && cmp(s[row], c)),
            ));
        }
    }

    let func_ctx = FunctionContext::default();
    // Constants are folded as the planner folds them, e.g. a cast of a constant to nullable.
    let fold = |text: &str| {
        let expr = type_check::check(
            &parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS),
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS)
            .0
            .into_owned()
    };
    for (text, expected) in cases {
        let expr = fold(&text);
        let expected = (0..num_rows)
            .filter(|row| expected(*row))
            .map(|row| row as i32)
            .collect::<Vec<_>>();
        for (i, block) in blocks.iter().enumerate() {
            let mut executor = FilterExecutor::new(
                expr.clone(),
                func_ctx.clone(),
                num_rows,
                None,
                &BUILTIN_FUNCTIONS,
                true,
            );
            let actual = executor.filter(block.clone()).unwrap();
            assert_eq!(
                actual.get_by_offset(0).value(),
                Value::Column(Int32Type::from_data(expected.clone())),
                "{text}, block {i}"
            );
        }
    }

    // The encoded blocks reach the dictionary comparison rather than decoding.
    for text in [
        "s = 'abcd'",
        "'abcd' < s",
        "n >= 'abcd'",
        "s = t",
        "s < t",
        "n > t",
    ] {
        let expr = fold(text);
        let Expr::FunctionCall(FunctionCall { function, args, .. }) = &expr else {
            unreachable!("{text}")
        };
        let op = SelectOp::try_from_func_name(&function.signature.name).unwrap();
        for block in &blocks[1..] {
            let evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
            assert!(
                evaluator
                    .try_select_compare_encoded(&op, args, None)
                    .is_some(),
                "{text}"
            );
        }
        let evaluator = Evaluator::new(&blocks[0], &func_ctx, &BUILTIN_FUNCTIONS);
        assert!(
            evaluator
                .try_select_compare_encoded(&op, args, None)
                .is_none(),
            "{text}"
        );
    }
}
