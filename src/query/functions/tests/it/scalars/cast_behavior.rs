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

//! The rules that every cast follows, by behavior. `test_cast_behavior` records an example of
//! each rule in `cast_behavior.txt`; `test_cast_invariants` checks the rules that hold for every
//! cast between the sample types.

use std::io::Write;

use databend_common_exception::ErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::cast_plan::resolve_cast_plan;
use databend_common_expression::type_check;
use databend_common_expression::types::*;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;

use super::parser;
use super::run_ast;
use super::write_file_heading;
use super::write_section;

#[test]
fn test_cast_behavior() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast_behavior.txt").unwrap();
    write_file_heading(
        file,
        "Rules that every cast follows",
        "Each section shows one rule with a few cases. The rules that hold for every pair of types are \
         also checked over all sample types by `test_cast_invariants`, without a golden file.",
    );

    test_cast_without_implementation(file);
    test_try_cast_fails_by_row(file);
    test_try_cast_converts_like_cast(file);
    test_cast_through_intermediate_type(file);
    test_cast_nulls(file);
    test_cast_nested_values(file);
    test_cast_domains(file);
    test_try_to_functions(file);
}

fn test_cast_without_implementation(file: &mut impl Write) {
    write_section(
        file,
        "Casts without an implementation",
        "A cast without an implementation fails type checking, instead of failing on every row.",
    );
    run_ast(file, "CAST(1.5 AS DATE)", &[]);
    run_ast(file, "TRY_CAST(to_binary('ab') AS INT32)", &[]);
    run_ast(file, "CAST(to_interval('1 day') AS INT64)", &[]);
    run_ast(file, "TRY_CAST(parse_json('1') AS BITMAP)", &[]);
}

fn test_try_cast_fails_by_row(file: &mut impl Write) {
    write_section(
        file,
        "TRY_CAST fails by row",
        "`TRY_CAST` turns a row that fails into NULL and converts the other rows.",
    );
    let strings = &[(
        "s",
        StringType::from_data(vec!["1", "abc", "2147483648", "-1"]),
    )];
    run_ast(file, "TRY_CAST(s AS INT32)", strings);
    run_ast(file, "TRY_CAST(s AS UINT8)", strings);

    let dates = &[("s", StringType::from_data(vec!["2000-01-01", "invalid"]))];
    run_ast(file, "TRY_CAST(s AS DATE)", dates);
    run_ast(file, "TRY_CAST(s AS DATE) IS NULL", dates);
    run_ast(file, "TRY_CAST(s AS TIMESTAMP)", dates);

    let numbers = &[(
        "n",
        Int64Type::from_opt_data(vec![Some(1), Some(1000), None, Some(-129)]),
    )];
    run_ast(file, "TRY_CAST(n AS INT8)", numbers);

    let json = &[(
        "s",
        StringType::from_data(vec!["1", "\"a\"", "null", "1.5", "[1]"]),
    )];
    run_ast(file, "TRY_CAST(parse_json(s) AS INT32)", json);
}

fn test_try_cast_converts_like_cast(file: &mut impl Write) {
    write_section(
        file,
        "TRY_CAST converts like CAST",
        "`TRY_CAST` converts a value as `CAST` does, and only differs on the rows that fail.",
    );
    // Issue #21068: a decimal renders with its scale, not through Float64.
    let decimals = &[(
        "s",
        StringType::from_data(vec![
            "1.1",
            "-0.000000001",
            "12345678901234567890.123456789",
        ]),
    )];
    run_ast(file, "CAST(CAST(s AS DECIMAL(38, 9)) AS STRING)", decimals);
    run_ast(
        file,
        "TRY_CAST(CAST(s AS DECIMAL(38, 9)) AS STRING)",
        decimals,
    );

    run_ast(
        file,
        "TRY_CAST(to_timestamp_tz('2024-01-02 03:04:05 +0800') AS STRING)",
        &[],
    );
    run_ast(file, "TRY_CAST(to_interval('1 day 2 hours') AS STRING)", &[
    ]);
    run_ast(file, "TRY_CAST(to_bitmap('1,3') AS STRING)", &[]);

    let bitmaps = &[("s", StringType::from_data(vec!["1,3", "abc"]))];
    run_ast(file, "TRY_CAST(s AS BITMAP)", bitmaps);

    // A JSON value that is not an array converts to an empty array, by `CAST` and `TRY_CAST`.
    let json = &[(
        "s",
        StringType::from_data(vec!["[1,2]", "1", "{\"a\":1}", "[\"a\"]"]),
    )];
    run_ast(file, "CAST(parse_json(s) AS ARRAY(INT32))", json);
    run_ast(file, "TRY_CAST(parse_json(s) AS ARRAY(INT32))", json);
    run_ast(file, "TRY_CAST(parse_json(s) AS MAP(STRING, INT32))", json);
}

fn test_cast_through_intermediate_type(file: &mut impl Write) {
    write_section(
        file,
        "Casts through an intermediate type",
        "A cast without a function of its own converts through an intermediate type, and a row that fails in either step is NULL for `TRY_CAST`.",
    );
    let int64 = DataType::Number(NumberDataType::Int64);
    for (src, dest) in [
        (DataType::Date, DataType::Number(NumberDataType::Int8)),
        (
            DataType::Timestamp,
            DataType::Number(NumberDataType::Float32),
        ),
        (DataType::Number(NumberDataType::UInt64), DataType::Date),
        (DataType::Number(NumberDataType::UInt8), DataType::Bitmap),
        (
            DataType::Decimal(DecimalSize::new(10, 2).unwrap()),
            DataType::Boolean,
        ),
        (DataType::Date.wrap_nullable(), int64.wrap_nullable()),
    ] {
        let plan = resolve_cast_plan(&src, &dest, &BUILTIN_FUNCTIONS).unwrap();
        writeln!(file, "plan           : {src} -> {dest}: {plan}").unwrap();
    }
    writeln!(file).unwrap();

    let dates = &[("d", DateType::from_data(vec![-1, 1, 19724]))];
    run_ast(file, "CAST(d AS INT8)", dates);
    run_ast(file, "TRY_CAST(d AS INT8)", dates);
    run_ast(file, "TRY_CAST(d AS UINT64)", dates);

    let numbers = &[("u", UInt64Type::from_data(vec![1, u64::MAX]))];
    run_ast(file, "CAST(u AS DATE)", numbers);
    run_ast(file, "TRY_CAST(u AS DATE)", numbers);
    run_ast(file, "TRY_CAST(u AS TIMESTAMP)", numbers);

    let small = &[("n", UInt8Type::from_data(vec![1, 255]))];
    run_ast(file, "TRY_CAST(n AS BITMAP)", small);
    run_ast(file, "CAST(n AS TIMESTAMP)", small);

    run_ast(file, "CAST(0.00::DECIMAL(10, 2) AS BOOLEAN)", &[]);
    run_ast(file, "TRY_CAST((-1.5)::DECIMAL(10, 2) AS BOOLEAN)", &[]);
}

fn test_cast_nulls(file: &mut impl Write) {
    write_section(
        file,
        "NULL",
        "A NULL converts to NULL. `CAST` to a type that is not nullable fails on a NULL, but only in a row that the cast converts.",
    );
    let numbers = &[("n", Int32Type::from_opt_data(vec![Some(1), None]))];
    run_ast(file, "CAST(n AS INT64)", numbers);
    run_ast(file, "TRY_CAST(n AS INT64)", numbers);
    run_ast(file, "CAST(n AS INT64 NULL)", numbers);

    run_ast(file, "CAST(NULL AS INT32)", &[]);
    run_ast(file, "TRY_CAST(NULL AS INT32)", &[]);
    run_ast(file, "CAST(parse_json('null') AS INT32)", &[]);
    run_ast(file, "CAST(parse_json('null') AS INT32 NULL)", &[]);
    run_ast(file, "TRY_CAST(parse_json('null') AS INT32)", &[]);

    let columns = &[
        ("c", BooleanType::from_data(vec![true, false])),
        ("s", StringType::from_data(vec!["1", "null"])),
    ];
    run_ast(file, "if(c, CAST(parse_json(s) AS INT32), 0)", columns);
    run_ast(file, "if(c, 0, CAST(parse_json(s) AS INT32))", columns);
}

fn test_cast_nested_values(file: &mut impl Write) {
    write_section(
        file,
        "Nested values",
        "A nested value converts by its elements. For `TRY_CAST`, an element that fails is NULL, and the other elements and rows are kept.",
    );
    let numbers = &[("n", Int64Type::from_data(vec![1, 1000]))];
    run_ast(file, "TRY_CAST([n, -1, 2] AS ARRAY(UINT8))", numbers);
    run_ast(file, "TRY_CAST({'a': n} AS MAP(STRING, INT8))", numbers);

    let strings = &[("s", StringType::from_data(vec!["1", "a"]))];
    run_ast(file, "TRY_CAST((s, s) AS TUPLE(INT32, STRING))", strings);

    run_ast(file, "TRY_CAST([] AS ARRAY(INT32))", &[]);
    run_ast(file, "CAST({} AS MAP(STRING, INT32))", &[]);
}

fn test_cast_domains(file: &mut impl Write) {
    write_section(
        file,
        "Domains",
        "The domain of a cast comes from the rule of each step. A `CAST` whose rows may fail has no domain, so that folding does not skip the error; a `TRY_CAST` may be NULL only when a row may fail.",
    );
    let dates = &[(
        "s",
        StringType::from_data(vec!["2022-01-02", "2022-01-02 01:00:00"]),
    )];
    run_ast(file, "CAST(s AS DATE)", dates);

    let wide = &[("n", Int64Type::from_data(vec![1, 1000]))];
    run_ast(file, "CAST(n AS INT8)", wide);
    run_ast(file, "TRY_CAST(n AS INT8)", wide);
    run_ast(file, "TRY_CAST(n AS STRING)", wide);

    let narrow = &[("n", Int64Type::from_data(vec![1, 100]))];
    run_ast(file, "CAST(n AS INT8)", narrow);
    run_ast(file, "TRY_CAST(n AS INT8)", narrow);

    let dates = &[("d", DateType::from_data(vec![1, 19724]))];
    run_ast(file, "TRY_CAST(d AS UINT8)", dates);

    write_section(
        file,
        "Domain of a single value",
        "A single value besides NULL converts exactly, by running the step that converts it.",
    );
    let single = &[("s", StringType::from_opt_data(vec![Some("1"), None]))];
    run_ast(file, "TRY_CAST(s AS DECIMAL(10, 2))", single);
    run_ast(file, "CAST(s AS DECIMAL(10, 2) NULL)", single);
    let invalid = &[("s", StringType::from_opt_data(vec![Some("a"), None]))];
    run_ast(file, "TRY_CAST(s AS DECIMAL(10, 2))", invalid);

    write_section(
        file,
        "Domain of maps",
        "The entries of a map convert as the elements of an array do.",
    );
    let maps = &[(
        "m",
        column_of(&[
            "{'a': 1}::MAP(STRING, INT64)",
            "{'b': 1000}::MAP(STRING, INT64)",
        ]),
    )];
    run_ast(file, "CAST(m AS MAP(STRING, INT16))", maps);
    run_ast(file, "TRY_CAST(m AS MAP(STRING, INT8))", maps);
}

/// A column of the values of `samples`, which must have the same type.
fn column_of(samples: &[&str]) -> Column {
    let values = samples
        .iter()
        .map(|sql| eval_sample(sql))
        .collect::<Vec<_>>();
    let mut builder = ColumnBuilder::with_capacity(&values[0].1, values.len());
    for (value, _) in &values {
        builder.push(value.as_ref());
    }
    builder.build()
}

fn test_try_to_functions(file: &mut impl Write) {
    write_section(
        file,
        "try_to_x functions",
        "`try_to_x` is an ordinary function that converts as its own overloads do, not a cast.",
    );
    let int64 = &[("n", Int64Type::from_data(vec![-1, i64::MAX]))];
    run_ast(file, "try_to_int64(n)", int64);
    let uint64 = &[("u", UInt64Type::from_data(vec![0, u64::MAX]))];
    run_ast(file, "try_to_uint64(u)", uint64);
    let strings = &[("s", StringType::from_data(vec!["1", "abc"]))];
    run_ast(file, "try_to_string(s)", strings);
    run_ast(file, "try_to_int8(s)", strings);
}

/// Each entry is one source type, given by SQL expressions for its sample values. All samples of
/// an entry must have the same type. Every source type except NULL is also a destination.
const SOURCES: &[&[&str]] = &[
    &["NULL"],
    &["true", "false"],
    &["'1'", "'-1.5'", "'abc'", "'2024-01-02 03:04:05'", "'[1,2]'"],
    &["to_binary('12')", "from_hex('ff')"],
    &["0::UINT8", "255::UINT8"],
    &["65535::UINT16"],
    &["4294967295::UINT32"],
    &["0::UINT64", "18446744073709551615::UINT64"],
    &["(-128)::INT8", "127::INT8"],
    &["(-32768)::INT16"],
    &["(-1)::INT32", "2147483647::INT32"],
    &["(-1)::INT64", "9223372036854775807::INT64"],
    &["1.5::FLOAT32", "'nan'::FLOAT32"],
    &["(-1.5)::FLOAT64", "1e300::FLOAT64"],
    &["0.00::DECIMAL(10, 2)", "(-12345678.90)::DECIMAL(10, 2)"],
    &[
        "1.1::DECIMAL(76, 9)",
        "12345678901234567890.123456789::DECIMAL(76, 9)",
    ],
    &["'1970-01-02'::DATE", "'2024-01-02'::DATE"],
    &["'2024-01-02 03:04:05.123456'::TIMESTAMP"],
    &["to_timestamp_tz('2024-01-02 03:04:05 +0800')"],
    &["to_interval('1 day 2 hours')"],
    &[
        "parse_json('1')",
        "parse_json('1.5')",
        "parse_json('\"abc\"')",
        "parse_json('null')",
        "parse_json('[1,2]')",
        "parse_json('{\"a\":1}')",
    ],
    &["to_bitmap('1,3')"],
    &["to_geometry('POINT(1 2)')"],
    &["to_geography('POINT(1 2)')"],
    &["[]"],
    &["[1, 2]::ARRAY(INT32)"],
    &["{}"],
    &["{'a': 1}::MAP(STRING, INT32)"],
    &["(1, 'a')::TUPLE(INT32, STRING)"],
    &["CAST(1 AS INT32 NULL)", "CAST(NULL AS INT32 NULL)"],
    &["CAST('1' AS STRING NULL)", "CAST(NULL AS STRING NULL)"],
    &[
        "CAST(parse_json('1') AS VARIANT NULL)",
        "CAST(NULL AS VARIANT NULL)",
    ],
];

/// For every cast between the sample types:
/// - `CAST` and `TRY_CAST` pass type checking together;
/// - `TRY_CAST` does not fail;
/// - a value converts to the same result as a column and as a constant;
/// - a column converts row by row: `TRY_CAST` returns the result of each row on its own, and
///   `CAST` fails when one of its rows fails;
/// - `TRY_CAST` returns what `CAST` returns for a value that `CAST` converts.
#[test]
fn test_cast_invariants() {
    let sources = SOURCES
        .iter()
        .map(|samples| {
            let values = samples
                .iter()
                .map(|sql| eval_sample(sql))
                .collect::<Vec<_>>();
            for (sql, (_, data_type)) in samples.iter().zip(&values) {
                assert_eq!(data_type, &values[0].1, "sample `{sql}` has another type");
            }
            values
        })
        .collect::<Vec<_>>();
    let dests = sources
        .iter()
        .map(|samples| samples[0].1.clone())
        .filter(|data_type| data_type != &DataType::Null)
        .collect::<Vec<_>>();

    let mut violations = Vec::new();
    for samples in &sources {
        let src_type = &samples[0].1;
        let scalars = samples
            .iter()
            .map(|(scalar, _)| scalar.clone())
            .collect::<Vec<_>>();
        for dest_type in &dests {
            if src_type == dest_type {
                continue;
            }
            let mut violation = |rule: &str| {
                violations.push(format!("{src_type} -> {dest_type}: {rule}"));
            };
            let (cast, try_cast) = match (
                check_cast(false, src_type, dest_type),
                check_cast(true, src_type, dest_type),
            ) {
                (Some(cast), Some(try_cast)) => (cast, try_cast),
                (None, None) => continue,
                _ => {
                    violation("CAST and TRY_CAST differ in type checking");
                    continue;
                }
            };

            let mut cast_rows = Vec::new();
            let mut try_cast_rows = Vec::new();
            for scalar in &scalars {
                let row = std::slice::from_ref(scalar);
                let cast_row = eval(&cast, src_type, row, false);
                let try_cast_row = eval(&try_cast, src_type, row, false);
                if eval(&cast, src_type, row, true) != cast_row
                    || eval(&try_cast, src_type, row, true) != try_cast_row
                {
                    violation("a constant converts unlike a column");
                }
                match (&cast_row, &try_cast_row) {
                    (_, Err(err)) => violation(&format!("TRY_CAST fails: {err}")),
                    (Ok(cast_row), Ok(try_cast_row)) if cast_row != try_cast_row => {
                        violation("TRY_CAST returns another value than CAST");
                    }
                    _ => {}
                }
                cast_rows.push(cast_row);
                try_cast_rows.push(try_cast_row);
            }
            if scalars.len() > 1 {
                let rows = |rows: Vec<Result<Vec<String>, String>>| {
                    rows.into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .map(|rows| rows.concat())
                };
                let cast_column = eval(&cast, src_type, &scalars, false);
                match (cast_column, rows(cast_rows)) {
                    (Ok(column), Ok(rows)) if column == rows => {}
                    (Err(_), Err(_)) => {}
                    _ => violation("CAST of a column differs from CAST of its rows"),
                }
                if eval(&try_cast, src_type, &scalars, false) != rows(try_cast_rows) {
                    violation("TRY_CAST of a column differs from TRY_CAST of its rows");
                }
            }
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

fn eval_sample(sql: &str) -> (Scalar, DataType) {
    let raw_expr = parser::parse_raw_expr(sql, &[], &BUILTIN_FUNCTIONS);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)
        .unwrap_or_else(|err| panic!("sample `{sql}`: {}", err.message()));
    let block = DataBlock::new(vec![], 1);
    let func_ctx = FunctionContext::default();
    let value = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
        .run(&expr)
        .unwrap_or_else(|err| panic!("sample `{sql}`: {}", err.message()));
    let scalar = match value {
        Value::Scalar(scalar) => scalar,
        Value::Column(column) => column.index(0).unwrap().to_owned(),
    };
    (scalar, expr.data_type().clone())
}

/// The checked cast of column 0, or `None` when type checking rejects it.
fn check_cast(is_try: bool, src_type: &DataType, dest_type: &DataType) -> Option<Expr> {
    let raw_expr = RawExpr::Cast {
        span: None,
        is_try,
        expr: Box::new(RawExpr::ColumnRef {
            span: None,
            id: 0,
            data_type: src_type.clone(),
            display_name: "x".to_string(),
        }),
        dest_type: dest_type.clone(),
    };
    type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).ok()
}

/// Converts `values` as one column, or as a constant when `constant` is set and there is one
/// value. Returns each row of the result, or the error.
fn eval(
    expr: &Expr,
    src_type: &DataType,
    values: &[Scalar],
    constant: bool,
) -> Result<Vec<String>, String> {
    let entry = if constant {
        assert_eq!(values.len(), 1);
        BlockEntry::new_const_column(src_type.clone(), values[0].clone(), 1)
    } else {
        let mut builder = ColumnBuilder::with_capacity(src_type, values.len());
        for value in values {
            builder.push(value.as_ref());
        }
        BlockEntry::from(builder.build())
    };
    let block = DataBlock::new(vec![entry], values.len());
    let func_ctx = FunctionContext::default();
    let value = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS)
        .run(expr)
        .map_err(|err: ErrorCode| err.message())?;
    let column: Column = match value {
        Value::Scalar(scalar) => {
            ColumnBuilder::repeat(&scalar.as_ref(), values.len(), expr.data_type()).build()
        }
        Value::Column(column) => column,
    };
    Ok(column.iter().map(|value| value.to_string()).collect())
}
