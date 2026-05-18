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

use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::RawExpr;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::stat_distribution::StatEstimate;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::ColumnStatSet;
use databend_common_sql::optimizer::ir::SelectivityEstimator;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::CastExpr;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql_test_support::parse_raw_expr;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_common_statistics::TypedHistogram;
use databend_common_statistics::TypedHistogramBucket;

use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

fn run_case(
    file: &mut impl Write,
    expr_text: &str,
    columns: &[(&str, DataType)],
    column_stats: ColumnStatSet,
) -> Result<()> {
    run_case_with_predicates(
        file,
        &[expr_text],
        columns,
        column_stats,
        StatCardinality::estimate(100.0),
    )
}

fn run_case_with_predicates(
    file: &mut impl Write,
    expr_texts: &[&str],
    columns: &[(&str, DataType)],
    column_stats: ColumnStatSet,
    cardinality: StatCardinality,
) -> Result<()> {
    writeln!(file, "expr          : {}", expr_texts.join(", "))?;

    let in_stats = column_stats_to_string(&column_stats);
    let exprs = expr_texts
        .iter()
        .map(|expr_text| {
            let raw_expr = parse_raw_expr(expr_text, columns, &BUILTIN_FUNCTIONS);
            raw_expr_to_scalar(&raw_expr, columns)
        })
        .collect::<Vec<_>>();
    let mut estimator = SelectivityEstimator::new(column_stats, cardinality);
    let estimated_rows = estimator.apply(&exprs)?;
    let out_stats = estimator.column_stats();

    writeln!(
        file,
        "cardinality   : {}",
        cardinality_to_string(cardinality)
    )?;
    writeln!(file, "estimated     : {estimated_rows}")?;
    writeln!(file, "in stats      :\n{in_stats}")?;
    writeln!(
        file,
        "out stats     :\n{}",
        column_stats_to_string(&out_stats)
    )?;
    writeln!(file)?;

    Ok(())
}

fn cardinality_to_string(cardinality: StatCardinality) -> String {
    match cardinality {
        StatCardinality::Exact(value) => format!("exact {value}"),
        StatCardinality::Estimate(value) => value.to_string(),
    }
}

fn column_stats_to_string(column_stats: &ColumnStatSet) -> String {
    let mut keys = column_stats.keys().copied().collect::<Vec<_>>();
    keys.sort();

    keys.iter()
        .map(|i| format!("{i} {:?}", column_stats[i]))
        .collect::<Vec<_>>()
        .join("\n")
}

fn raw_expr_to_scalar(raw_expr: &RawExpr, columns: &[(&str, DataType)]) -> ScalarExpr {
    match raw_expr {
        RawExpr::Constant { scalar, .. } => ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: scalar.clone(),
        }),
        RawExpr::ColumnRef { id, .. } => {
            let index = *id;
            let (name, data_type) = &columns[index];
            let column = ColumnBindingBuilder::new(
                name.to_string(),
                Symbol::new(index),
                Box::new(data_type.clone()),
                Visibility::Visible,
            )
            .build();
            ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
        }
        RawExpr::Cast {
            expr,
            dest_type,
            is_try,
            ..
        } => ScalarExpr::CastExpr(CastExpr {
            span: None,
            is_try: *is_try,
            argument: Box::new(raw_expr_to_scalar(expr, columns)),
            target_type: Box::new(dest_type.clone()),
        }),
        RawExpr::FunctionCall {
            name, args, params, ..
        } => ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: name.clone(),
            params: params.clone(),
            arguments: args
                .iter()
                .map(|arg| raw_expr_to_scalar(arg, columns))
                .collect(),
        }),
        RawExpr::LambdaFunctionCall { .. } => {
            unreachable!("lambda expressions are not used in tests")
        }
    }
}

#[test]
fn test_selectivity_comparison_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity_comparison.txt")?;
    write_case_title(
        &mut file,
        "comparison_predicates",
        "Comparison predicates should update estimated rows and column stats consistently.",
    )?;
    let comparison_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(10),
            max: Datum::UInt(20),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(10),
            max: Datum::UInt(20),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(10),
            histogram: None,
        }),
    ]);
    let comparison_columns = &[("a", UInt64Type::data_type())];
    for expr in [
        "a = 5",
        "a = 15",
        "a != 5",
        "a != 15",
        "a > 5",
        "a > 10",
        "a > 17",
        "a > 20",
        "a > 25",
        "a >= 5",
        "a >= 10",
        "a >= 17",
        "a >= 20",
        "a >= 25",
        "a < 5",
        "a < 10",
        "a < 17",
        "a < 20",
        "a < 25",
        "a <= 5",
        "a <= 10",
        "a <= 17",
        "a <= 20",
        "a <= 25",
        "a + 1 = 15",
    ] {
        run_case(
            &mut file,
            expr,
            comparison_columns,
            comparison_stats.clone(),
        )?;
    }

    write_case_title(
        &mut file,
        "reversed_comparison_predicates",
        "Comparison predicates should apply the same column constraint when the constant is on the left.",
    )?;
    for expr in ["15 = a", "17 < a", "17 <= a", "17 > a", "17 >= a"] {
        run_case(
            &mut file,
            expr,
            comparison_columns,
            comparison_stats.clone(),
        )?;
    }

    write_case_title(
        &mut file,
        "typed_comparison_predicates",
        "Typed comparison predicates should respect integer boundaries, nullable inputs, and histogram constants.",
    )?;
    let int_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(1),
        max: Datum::Int(10),
        ndv: StatEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in ["i < 5", "i > 5"] {
        run_case(
            &mut file,
            expr,
            &[("i", Int64Type::data_type())],
            int_stats.clone(),
        )?;
    }
    let nullable_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(1),
        max: Datum::Int(10),
        ndv: StatEstimate::exact(10.0),
        null_count: StatCount::exact(30),
        histogram: None,
    })]);
    run_case(
        &mut file,
        "n > 5",
        &[("n", Int64Type::data_type().wrap_nullable())],
        nullable_stats,
    )?;
    let edge_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(1),
        max: Datum::Int(10),
        ndv: StatEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::Int(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(1, 10, 100.0, 10.0)],
            avg_spacing: None,
        })),
    })]);
    for expr in ["h >= 10", "h < 10"] {
        run_case(
            &mut file,
            expr,
            &[("h", Int64Type::data_type())],
            edge_histogram_stats.clone(),
        )?;
    }
    let uint8_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(10),
        ndv: StatEstimate::exact(11.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(0, 10, 100.0, 11.0)],
            avg_spacing: None,
        })),
    })]);
    run_case(
        &mut file,
        "u > 5",
        &[("u", UInt8Type::data_type())],
        uint8_histogram_stats,
    )?;

    write_case_title(
        &mut file,
        "string_comparison_predicates",
        "String equality should use domain and function statistics without assuming unbounded strings are impossible.",
    )?;
    let string_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Bytes(b"b".to_vec()),
        max: Datum::Bytes(b"d".to_vec()),
        ndv: StatEstimate::exact(3.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in ["s = 'a'", "s = 'c'", "'c' = s", "s != 'c'"] {
        run_case(
            &mut file,
            expr,
            &[("s", DataType::String)],
            string_stats.clone(),
        )?;
    }

    Ok(())
}

#[test]
fn test_selectivity_logical_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity_logical.txt")?;
    write_case_title(
        &mut file,
        "logical_predicates",
        "Logical predicate composition should combine selectivity estimates and null handling.",
    )?;
    let logical_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(10),
            histogram: None,
        }),
    ]);
    for expr in [
        "and_filters(a = 5, a > 3)",
        "or_filters(a = 5, a = 6)",
        "not(a = 5)",
    ] {
        run_case(
            &mut file,
            expr,
            &[("a", UInt64Type::data_type())],
            logical_stats.clone(),
        )?;
    }
    run_case(
        &mut file,
        "is_not_null(b)",
        &[
            ("a", UInt64Type::data_type()),
            ("b", UInt64Type::data_type().wrap_nullable()),
        ],
        logical_stats.clone(),
    )?;
    run_case_with_predicates(
        &mut file,
        &["a > 3", "a < 6"],
        &[("a", UInt64Type::data_type())],
        logical_stats.clone(),
        StatCardinality::estimate(100.0),
    )?;

    write_case_title(
        &mut file,
        "constant_logical_predicates",
        "Constant predicates should preserve Zero and All through logical composition.",
    )?;
    for expr in ["1", "0", "-1", "true", "false", "not(false)", "not(true)"] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[],
            ColumnStatSet::new(),
            StatCardinality::estimate(4.0),
        )?;
    }

    let nested_constant_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(3),
        ndv: StatEstimate::exact(4.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in [
        "and_filters(a = 1, true)",
        "and_filters(a = 1, false)",
        "or_filters(a = 1, true)",
        "or_filters(a = 1, false)",
        "or_filters(and_filters(a = 1, true), and_filters(a = 2, false))",
    ] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[("a", UInt64Type::data_type())],
            nested_constant_stats.clone(),
            StatCardinality::estimate(4.0),
        )?;
    }

    write_case_title(
        &mut file,
        "domain_folded_logical_predicates",
        "Domain-folded constants should still compose with remaining predicate selectivity.",
    )?;
    let domain_folded_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(0),
            ndv: StatEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(0),
            ndv: StatEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(2), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(4),
            ndv: StatEstimate::exact(5.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(3), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(4),
            ndv: StatEstimate::exact(5.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
    ]);
    run_case_with_predicates(
        &mut file,
        &["or_filters(and_filters(a > 1, c > 2), and_filters(b < 3, d < 4))"],
        &[
            ("a", UInt64Type::data_type()),
            ("b", UInt64Type::data_type()),
            ("c", UInt64Type::data_type()),
            ("d", UInt64Type::data_type()),
        ],
        domain_folded_stats,
        StatCardinality::estimate(4.4),
    )?;

    Ok(())
}

#[test]
fn test_selectivity_null_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity_null.txt")?;
    write_case_title(
        &mut file,
        "estimated_zero_predicates",
        "Estimated zero selectivity should not be treated as a proven empty result.",
    )?;
    let estimated_zero_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(5),
        ndv: StatEstimate::exact(6.0),
        null_count: StatCount::estimate(8.0, 8.0),
        histogram: None,
    })]);
    run_case_with_predicates(
        &mut file,
        &["is_not_null(n)"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        estimated_zero_stats,
        StatCardinality::estimate(8.0),
    )?;
    let estimated_zero_cardinality_stats =
        ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(5),
            ndv: StatEstimate::exact(6.0),
            null_count: StatCount::exact(2),
            histogram: Some(Histogram::UInt(TypedHistogram {
                accuracy: false,
                buckets: vec![TypedHistogramBucket::new(0, 5, 6.0, 6.0)],
                avg_spacing: None,
            })),
        })]);
    run_case_with_predicates(
        &mut file,
        &["is_not_null(n)"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        estimated_zero_cardinality_stats,
        StatCardinality::estimate(0.0),
    )?;

    write_case_title(
        &mut file,
        "exact_null_predicates",
        "Exact all-null input domains should fold is_not_null to an empty result and clear distributions.",
    )?;
    let exact_all_null_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(5),
        ndv: StatEstimate::exact(6.0),
        null_count: StatCount::exact(8),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: false,
            buckets: vec![TypedHistogramBucket::new(0, 5, 6.0, 6.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["is_not_null(n)"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        exact_all_null_stats,
        StatCardinality::exact(8),
    )?;

    Ok(())
}

#[test]
fn test_selectivity_special_predicate_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity_special_predicate.txt")?;
    write_case_title(
        &mut file,
        "modulo_predicates",
        "Modulo predicates should narrow value ranges only when the comparison is satisfiable.",
    )?;
    let mod_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: StatEstimate::exact(10.0),
            null_count: StatCount::exact(10),
            histogram: None,
        }),
    ]);
    for expr in ["a % 4 = 1", "a % 4 = 5"] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[("a", UInt64Type::data_type())],
            mod_stats.clone(),
            StatCardinality::estimate(100.0),
        )?;
    }

    write_case_title(
        &mut file,
        "like_predicates",
        "LIKE predicates should use string-domain statistics when estimating selectivity.",
    )?;
    let like_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Bytes("aa".as_bytes().to_vec()),
        max: Datum::Bytes("zz".as_bytes().to_vec()),
        ndv: StatEstimate::exact(52.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in ["s like 'ab%'", "s like '%ab_'"] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[("s", DataType::String)],
            like_stats.clone(),
            StatCardinality::estimate(100.0),
        )?;
    }

    Ok(())
}
