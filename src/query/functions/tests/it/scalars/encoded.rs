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

//! Expressions over encoded columns must evaluate exactly like over the decoded columns,
//! whether the function reads the encoding directly (decimal arithmetic) or not.

use std::borrow::Cow;

use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::EvaluateOptions;
use databend_common_expression::Evaluator;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::type_check;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::ForColumn;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NarrowBuffer;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::parser;

fn decimal(values: &[i64], precision: u8, scale: u8) -> (Column, Column) {
    let size = DecimalSize::new_unchecked(precision, scale);
    let plain = Column::Decimal(DecimalColumn::Decimal64(values.to_vec().into(), size));
    (encode(&plain), plain)
}

fn encode(plain: &Column) -> Column {
    Column::Encoded(Box::new(EncodedColumn::For(
        ForColumn::try_encode(plain).unwrap(),
    )))
}

fn build_dict(plain: &Column) -> Column {
    let dict = DictColumn::try_encode(plain, 1 << 16).unwrap();
    Column::Encoded(Box::new(EncodedColumn::Dict(dict)))
}

fn dict(values: &[&str], keys: &[u32]) -> (Column, Column) {
    let dictionary = StringType::from_data(values.to_vec());
    let plain = StringType::from_data(keys.iter().map(|k| values[*k as usize]).collect());
    let keys = NarrowBuffer::pack(keys.iter().map(|k| *k as u64), values.len() as u64 - 1);
    let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
        keys, dictionary,
    ))));
    (encoded, plain)
}

#[test]
fn test_eval_on_encoded_columns() {
    let n = 257;
    let price = (0..n).map(|i| 90_000 + i * 4099).collect::<Vec<i64>>();
    let discount = (0..n).map(|i| i % 11).collect::<Vec<i64>>();
    let tax = (0..n).map(|i| i % 9).collect::<Vec<i64>>();
    // non-zero divisor
    let quantity = (0..n).map(|i| 100 + i % 50 * 100).collect::<Vec<i64>>();
    let narrow = (0..n).map(|i| i * 3 - 200).collect::<Vec<i64>>();
    let flags = (0..n).map(|i| (i % 3) as u32).collect::<Vec<_>>();
    let dates = (0..n as i32)
        .map(|i| 10_400 + (i * 7) % 200)
        .collect::<Vec<_>>();
    let quantities = (0..n as i32).map(|i| 1 + i % 50).collect::<Vec<_>>();
    let codes = (0..n)
        .map(|i| [7, 1_000_000_007, -3][i as usize % 3])
        .collect::<Vec<i64>>();
    let ratios = (0..n)
        .map(|i| [0.5, -2.25, 1e9][i as usize % 3])
        .collect::<Vec<f64>>();
    let amounts = (0..n)
        .map(|i| [100, 250_000_000_000][i as usize % 2])
        .collect::<Vec<i64>>();

    let columns = [
        ("price", decimal(&price, 15, 2)),
        ("discount", decimal(&discount, 15, 2)),
        ("tax", decimal(&tax, 15, 2)),
        ("quantity", decimal(&quantity, 15, 2)),
        ("narrow", decimal(&narrow, 6, 3)),
        // Clamped arithmetic binds these at scales 0/20, not 20/20.
        // Small coefficients still admit FOR encoding despite Decimal128's
        // wide logical precision, which is essential to exercise the bug.
        ("whole", {
            let plain = Column::Decimal(DecimalColumn::Decimal128(
                (0..n).map(|i| i as i128 + 1).collect(),
                DecimalSize::new(38, 0).unwrap(),
            ));
            (encode(&plain), plain)
        }),
        ("tiny", {
            let plain = Column::Decimal(DecimalColumn::Decimal128(
                (0..n).map(|i| i as i128 + 1).collect(),
                DecimalSize::new(38, 20).unwrap(),
            ));
            (encode(&plain), plain)
        }),
        ("nullable_whole", {
            let plain = Column::Decimal(DecimalColumn::Decimal128(
                (0..n).map(|i| i as i128 + 1).collect(),
                DecimalSize::new(38, 0).unwrap(),
            ));
            let validity = (0..n).map(|i| i % 3 != 0).collect::<Bitmap>();
            (
                encode(&plain).wrap_nullable(Some(validity.clone())),
                plain.wrap_nullable(Some(validity)),
            )
        }),
        ("flag", dict(&["A", "N", "R"], &flags)),
        ("ship", {
            let plain = DateType::from_data(dates);
            (encode(&plain), plain)
        }),
        ("qty", {
            let plain = Int32Type::from_data(quantities);
            (encode(&plain), plain)
        }),
        // block-level dictionaries over wide-range / non-integer values
        ("code", {
            let plain = Int64Type::from_data(codes);
            (build_dict(&plain), plain)
        }),
        ("ratio", {
            let plain = Float64Type::from_data(ratios);
            (build_dict(&plain), plain)
        }),
        ("amount", {
            let plain = Column::Decimal(DecimalColumn::Decimal64(
                amounts.into(),
                DecimalSize::new_unchecked(15, 2),
            ));
            (build_dict(&plain), plain)
        }),
    ];

    // (expression, whether the result stays encoded)
    let exprs = [
        ("price + discount", true),
        ("price - discount", true),
        ("1 - discount", true),
        ("discount + 1", true),
        ("price * (1 - discount)", true),
        ("price * (1 - discount) * (1 + tax)", true),
        ("price * discount", true),
        ("price / quantity", false),
        ("price % quantity", false),
        // different scales
        ("price + narrow", false),
        ("narrow * narrow", true),
        ("price + 1.5", true),
        ("price * 2", true),
        // Aligned plans must decode instead of combining raw coefficients.
        ("whole + 0.00000000000000000001::DECIMAL(38,20)", false),
        ("whole - 0.00000000000000000001::DECIMAL(38,20)", false),
        ("0.00000000000000000001::DECIMAL(38,20) - whole", false),
        ("whole + tiny", false),
        ("tiny + whole", false),
        ("whole - tiny", false),
        ("tiny - whole", false),
        ("nullable_whole + tiny", false),
        ("nullable_whole - tiny", false),
        ("(whole + whole) - tiny", false),
        // Checked same-scale/integral plans still have the FOR backend.
        ("whole + whole", true),
        ("whole - whole", true),
        ("whole * whole", true),
        ("whole * 0.00000000000000000001::DECIMAL(38,20)", true),
        ("price > quantity", false),
        ("price::DECIMAL(20, 4)", false),
        ("floor(price * discount)", false),
        ("flag = 'N'", false),
        ("concat(flag, 'x')", false),
        ("if(flag = 'A', price, discount)", false),
        ("ship", true),
        ("ship <= '1998-09-02'::DATE", false),
        ("'1998-09-02'::DATE > ship", false),
        ("ship = '1998-08-01'::DATE", false),
        ("ship >= '2000-01-01'::DATE", false),
        ("qty < 25", false),
        ("qty != 7", false),
        ("qty + 1", false),
        ("price * discount > 1000", false),
        ("ship <= '1998-09-02'::DATE and qty < 25", false),
        ("code", true),
        ("code + 1", true),
        ("code = 7", false),
        ("code > 0 and ratio < 1", false),
        ("ratio * 2", true),
        ("amount * (1 - discount)", false),
        ("amount + price", false),
        ("amount > 1000", false),
    ];

    let names = columns
        .iter()
        .map(|(name, (encoded, _))| (*name, encoded.data_type()))
        .collect::<Vec<_>>();
    let encoded_block = DataBlock::new(
        columns
            .iter()
            .map(|(_, (encoded, _))| BlockEntry::from(encoded.clone()))
            .collect(),
        n as usize,
    );
    let plain_block = DataBlock::new(
        columns
            .iter()
            .map(|(_, (_, plain))| BlockEntry::from(plain.clone()))
            .collect(),
        n as usize,
    );

    let func_ctx = FunctionContext::default();
    for (text, stays_encoded) in exprs {
        let raw_expr = parser::parse_raw_expr(text, &names, &BUILTIN_FUNCTIONS);
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let expr = type_check::rewrite_function_to_cast(expr, &BUILTIN_FUNCTIONS);
        // Fold constants like the planner does, so comparisons see constant operands.
        let (expr, _) = ConstantFolder::fold(Cow::Owned(expr), &func_ctx, &BUILTIN_FUNCTIONS);
        let expr = expr.into_owned();

        let encoded_evaluator = Evaluator::new(&encoded_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let plain_evaluator = Evaluator::new(&plain_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let plain = plain_evaluator.run(&expr).unwrap();

        let encoded = encoded_evaluator.run(&expr).unwrap();
        assert!(!encoded.is_encoded(), "{text}: result must be decoded");
        assert_eq!(encoded, plain, "{text}");

        let kept = encoded_evaluator
            .partial_run(&expr, None, &mut EvaluateOptions::default(), true)
            .unwrap();
        assert_eq!(kept.is_encoded(), stays_encoded, "{text}");
        assert_eq!(kept.decode_owned(), plain, "{text}");

        // The selector (filter operator) agrees on predicates.
        if expr.data_type() == &DataType::Boolean {
            let count = |block: &DataBlock| {
                FilterExecutor::new(
                    expr.clone(),
                    func_ctx.clone(),
                    n as usize,
                    None,
                    &BUILTIN_FUNCTIONS,
                    false,
                )
                .select(block)
                .unwrap()
            };
            assert_eq!(count(&encoded_block), count(&plain_block), "{text}");
        }
    }
}

/// Compare both physical representations, including partial evaluation and decoded callers.
fn eval_dictionary(
    text: &str,
    encoded: Column,
    plain: Column,
    validity: Option<Bitmap>,
    stays_encoded: bool,
) -> Value<databend_common_expression::types::AnyType> {
    let names = [("s", plain.data_type())];
    let raw_expr = parser::parse_raw_expr(text, &names, &BUILTIN_FUNCTIONS);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    let func_ctx = FunctionContext::default();
    let evaluate = |column: Column, keep_encoded| {
        let block = DataBlock::new(vec![BlockEntry::from(column.clone())], column.len());
        Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
            .partial_run(
                &expr,
                validity.clone(),
                &mut EvaluateOptions::default(),
                keep_encoded,
            )
            .unwrap()
    };
    let expected = evaluate(plain, false);
    let result = evaluate(encoded.clone(), true);
    assert_eq!(result.is_encoded(), stays_encoded, "{text}");
    assert_eq!(result.clone().decode_owned(), expected, "{text}");
    assert_eq!(evaluate(encoded, false), expected, "{text}");
    result
}

#[test]
fn test_dictionary_function_evaluation() {
    let values = ["https://a.com/x", "https://a.com/y", "https://b.com/z"];
    let keys = [0, 1, 2, 0, 1, 0, 2, 1];
    let (encoded, plain) = dict(&values, &keys);
    for text in [
        "octet_length(s)",
        "octet_length(regexp_replace(s, '^https://([^/]+)/.*$', '$1'))",
        "regexp_replace('prefix', 'fix', s)",
    ] {
        eval_dictionary(text, encoded.clone(), plain.clone(), None, true);
    }

    // Mapping is not injective: codes 0 and 1 now represent the same value. Keep the
    // original keys rather than assuming dictionary entries must remain distinct.
    let result = eval_dictionary(
        "regexp_replace(s, '^https://([^/]+)/.*$', '$1')",
        encoded,
        plain,
        None,
        true,
    );
    let Value::Column(Column::Encoded(result)) = result else {
        unreachable!()
    };
    let result = result.as_dict().unwrap();
    assert_eq!(
        result.values,
        StringType::from_data(vec!["a.com", "a.com", "b.com"])
    );
    assert_eq!(result.keys.iter().collect::<Vec<_>>(), keys.map(u64::from));

    let (encoded, plain) = dict(&["é", "hello"], &[0, 1, 0, 1]);
    eval_dictionary("length(s)", encoded.clone(), plain.clone(), None, true);
    eval_dictionary("octet_length(s)", encoded, plain, None, true);
}

#[test]
fn test_dictionary_like() {
    let (encoded, plain) = dict(&["google_a", "googleXa", "example"], &[0, 1, 2, 0, 1, 0]);
    for text in ["s LIKE '%google%'", r#""like"(s, 'google!_%', '!')"#] {
        // The Boolean result is expanded: Boolean has no encoded views.
        eval_dictionary(text, encoded.clone(), plain.clone(), None, false);
    }
}

#[test]
fn test_dictionary_arithmetic() {
    let values = Int32Type::from_data(vec![i32::MIN, 0, 1, i32::MAX]);
    let keys = NarrowBuffer::pack([0, 1, 2, 3, 1, 0, 2, 3].into_iter(), 3);
    let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
        keys.clone(),
        values,
    ))));
    let plain = encoded.clone().decode_owned();
    for text in [
        "s + 1", "1 + s", "s - 1", "1 - s", "s * 2", "s / 2", "s DIV 2", "s % 3", "s % -1",
    ] {
        let result = eval_dictionary(text, encoded.clone(), plain.clone(), None, true);
        let Value::Column(Column::Encoded(result)) = result else {
            panic!("{text}")
        };
        assert!(result.as_dict().unwrap().keys.shares_storage(&keys));
    }
    eval_dictionary("s + s", encoded.clone(), plain.clone(), None, false);
    let valid = Bitmap::from_iter([true, false, true, true, true, false, true, true]);
    eval_dictionary(
        "s - 1",
        encoded.wrap_nullable(Some(valid.clone())),
        plain.wrap_nullable(Some(valid)),
        None,
        false,
    );
}

#[test]
fn test_dictionary_arithmetic_errors() {
    let values = Int32Type::from_data(vec![0, 2, 4]);
    for text in ["8 / s", "8 DIV s", "8 % s"] {
        for keys in [[1, 2, 1, 2], [1, 0, 2, 1]] {
            let encoded = Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
                NarrowBuffer::pack(keys.into_iter(), 2),
                values.clone(),
            ))));
            let plain = encoded.clone().decode_owned();
            if !keys.contains(&0) {
                eval_dictionary(text, encoded, plain, None, true);
                continue;
            }
            eval_dictionary(
                text,
                encoded.clone(),
                plain.clone(),
                Some(Bitmap::from_iter(keys.iter().map(|key| *key != 0))),
                false,
            );
            let raw_expr =
                parser::parse_raw_expr(text, &[("s", plain.data_type())], &BUILTIN_FUNCTIONS);
            let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
            let func_ctx = FunctionContext::default();
            let evaluate = |column: Column| {
                let block = DataBlock::new(vec![BlockEntry::from(column)], keys.len());
                Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
                    .run(&expr)
                    .unwrap_err()
            };
            let expected = evaluate(plain);
            let actual = evaluate(encoded);
            assert_eq!(actual.code(), expected.code());
            assert!(
                actual.message().starts_with(
                    expected
                        .message()
                        .split(" while evaluating function")
                        .next()
                        .unwrap(),
                )
            );
        }
    }
}

#[test]
fn test_dictionary_function_fallbacks() {
    let (encoded, plain) = dict(&["a", "b"], &[0, 1, 0, 1]);
    // Two column arguments, including the same dictionary passed twice, are not lifted.
    eval_dictionary(
        "regexp_replace(s, s, 'x')",
        encoded.clone(),
        plain.clone(),
        None,
        false,
    );
    // Existing kernels retain their partial-evaluation and nullable semantics.
    eval_dictionary(
        "octet_length(s)",
        encoded.clone(),
        plain.clone(),
        Some(Bitmap::from_iter([true, false, true, false])),
        false,
    );
    eval_dictionary(
        "octet_length(s)",
        encoded.wrap_nullable(Some(Bitmap::from_iter([true, false, true, true]))),
        plain.wrap_nullable(Some(Bitmap::from_iter([true, false, true, true]))),
        None,
        false,
    );
    for keys in [&[][..], &[0, 1][..]] {
        let (encoded, plain) = dict(&["a", "b"], keys);
        eval_dictionary("octet_length(s)", encoded, plain, None, false);
    }

    // Errors in unreferenced dictionary entries do not apply to any logical row.
    // Keep the dictionary result instead of decoding and evaluating the rows again.
    let (encoded, plain) = dict(&["", "not referenced"], &[0, 0, 0, 0]);
    eval_dictionary("regexp_replace(s, '[', 'x')", encoded, plain, None, true);
    let (encoded, plain) = dict(&["a", "["], &[0, 0, 0, 0]);
    eval_dictionary("regexp_replace('abc', s, 'x')", encoded, plain, None, true);
    let (encoded, plain) = dict(&["", "bad"], &[0, 1, 0, 1]);
    eval_dictionary(
        "is_not_error(regexp_replace(s, '[', 'x'))",
        encoded.clone(),
        plain.clone(),
        None,
        false,
    );
    eval_dictionary(
        "regexp_replace(s, '[', 'x')",
        encoded,
        plain,
        Some(Bitmap::from_iter([true, false, true, false])),
        false,
    );
}

#[test]
fn test_dictionary_function_error_diagnostics() {
    // Dictionary positions need not match logical rows. The first error message can
    // also belong to an unused entry with a different error from the referenced one.
    for (text, values, keys) in [
        ("regexp_replace(s, '[', 'x')", ["", "offending source"], [
            0, 0, 1, 0,
        ]),
        ("regexp_replace('abc', s, 'x')", ["(", "["], [1, 1, 1, 1]),
    ] {
        let (encoded, plain) = dict(&values, &keys);
        let raw_expr =
            parser::parse_raw_expr(text, &[("s", plain.data_type())], &BUILTIN_FUNCTIONS);
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let func_ctx = FunctionContext::default();
        let evaluate = |column: Column| {
            let block = DataBlock::new(vec![BlockEntry::from(column.clone())], column.len());
            Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
                .run(&expr)
                .unwrap_err()
        };
        let expected = evaluate(plain);
        let dictionary_error = evaluate(StringType::from_data(values.to_vec()));
        let actual = evaluate(encoded);
        assert_eq!(actual.code(), expected.code());
        // Compare the first kernel error, not the outer expression-rendering context.
        assert!(
            actual.message().starts_with(
                dictionary_error
                    .message()
                    .split(" while evaluating function")
                    .next()
                    .unwrap(),
            )
        );
    }
}

#[test]
fn test_dictionary_function_evaluates_entries_not_rows() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    static EVALUATED: AtomicUsize = AtomicUsize::new(0);
    let mut registry = FunctionRegistry::empty();
    registry.register_1_arg::<StringType, NumberType<u64>, _>(
        "counted_length",
        |_, _| FunctionDomain::Full,
        |value, _| {
            EVALUATED.fetch_add(1, Ordering::Relaxed);
            value.len() as u64
        },
    );
    registry.properties.insert(
        "counted_length".to_string(),
        FunctionProperty::default().dictionary_eval(),
    );
    let (encoded, _) = dict(&["a", "bb", "ccc"], &[0, 1, 2, 0, 1, 2, 0, 1]);
    let raw_expr = parser::parse_raw_expr(
        "counted_length(s)",
        &[("s", encoded.data_type())],
        &registry,
    );
    let expr = type_check::check(&raw_expr, &registry).unwrap();
    let block = DataBlock::new(vec![BlockEntry::from(encoded)], 8);
    let ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&block, &ctx, &registry);
    let result = evaluator
        .partial_run(&expr, None, &mut EvaluateOptions::default(), true)
        .unwrap();
    assert!(result.is_encoded());
    assert_eq!(EVALUATED.load(Ordering::Relaxed), 3);

    // Opt-in cannot override volatility. The normal row kernel must still run N times.
    registry
        .properties
        .get_mut("counted_length")
        .unwrap()
        .non_deterministic = true;
    EVALUATED.store(0, Ordering::Relaxed);
    let result = Evaluator::new(&block, &ctx, &registry)
        .partial_run(&expr, None, &mut EvaluateOptions::default(), true)
        .unwrap();
    assert!(!result.is_encoded());
    assert_eq!(EVALUATED.load(Ordering::Relaxed), 8);
}

/// The bits of a non-nullable float column (`Eq` on `F64` equates `+0.0` and `-0.0`).
fn float_bits(column: &Column) -> Vec<u64> {
    match column {
        Column::Number(NumberColumn::Float64(b)) => b.iter().map(|v| v.0.to_bits()).collect(),
        Column::Number(NumberColumn::Float32(b)) => {
            b.iter().map(|v| v.0.to_bits() as u64).collect()
        }
        column => panic!("not a float column: {column:?}"),
    }
}

/// A float dictionary must reproduce the exact values, not only SQL equal ones: signed zeros
/// (`atan2(-0.0, -1.0) = -pi`, `atan2(+0.0, -1.0) = pi`) and NaN payloads.
#[test]
fn test_float_dictionary_preserves_bits() {
    let n = 8;
    let f64s = [0.0, -0.0, 0.0, -0.0, f64::NAN, -f64::NAN, 1.5, -0.0];
    let f32s = [0.0f32, -0.0, -0.0, 0.0, f32::NAN, -f32::NAN, 1.5, -0.0];
    let plain_x = Float64Type::from_data(f64s.to_vec());
    let plain_y = Float32Type::from_data(f32s.to_vec());

    for plain in [&plain_x, &plain_y] {
        let encoded = build_dict(plain);
        let Column::Encoded(dict) = &encoded else {
            unreachable!()
        };
        // +0.0, -0.0, NaN, -NaN and 1.5 are all distinct dictionary values.
        assert_eq!(dict.as_dict().unwrap().values.len(), 5);
        assert_eq!(float_bits(&dict.decode()), float_bits(plain));
    }

    let names = [("x", plain_x.data_type()), ("y", plain_y.data_type())];
    let block = |x: &Column, y: &Column| {
        DataBlock::new(
            vec![BlockEntry::from(x.clone()), BlockEntry::from(y.clone())],
            n,
        )
    };
    let encoded_block = block(&build_dict(&plain_x), &build_dict(&plain_y));
    let plain_block = block(&plain_x, &plain_y);
    let func_ctx = FunctionContext::default();
    for text in ["atan2(x, -1.0)", "atan2(y, -1.0)", "x * 2.0"] {
        let raw_expr = parser::parse_raw_expr(text, &names, &BUILTIN_FUNCTIONS);
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let run = |block: &DataBlock| {
            Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS)
                .run(&expr)
                .unwrap()
                .convert_to_full_column(expr.data_type(), n)
        };
        let (encoded, plain) = (run(&encoded_block), run(&plain_block));
        assert_eq!(float_bits(&encoded), float_bits(&plain), "{text}");
    }
    // The sign of zero reaches the result.
    let atan2 = |x: f64| x.atan2(-1.0).to_bits();
    assert_ne!(atan2(-0.0), atan2(0.0));
}
