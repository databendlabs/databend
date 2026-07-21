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
use databend_common_expression::Scalar;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::ColumnStatSet;
use databend_common_sql::optimizer::ir::CountMinSketchSet;
use databend_common_sql::optimizer::ir::SelectivityEstimator;
use databend_common_sql::optimizer::ir::TopNSet;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::CastExpr;
use databend_common_sql::plans::ComparisonOp;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_statistics::Datum;
use databend_common_statistics::F64;
use databend_common_statistics::Histogram;
use databend_common_statistics::TypedHistogram;
use databend_common_statistics::TypedHistogramBucket;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::ColumnTopNEntry;

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
    let exprs = expr_texts
        .iter()
        .map(|expr_text| {
            let raw_expr = parse_raw_expr(expr_text, columns, &BUILTIN_FUNCTIONS);
            raw_expr_to_scalar(&raw_expr, columns)
        })
        .collect::<Vec<_>>();
    run_scalar_case_with_predicates(
        file,
        expr_texts,
        &exprs,
        column_stats,
        cardinality,
        None,
        None,
    )
}

fn run_scalar_case_with_predicates(
    file: &mut impl Write,
    expr_texts: &[&str],
    predicates: &[ScalarExpr],
    column_stats: ColumnStatSet,
    cardinality: StatCardinality,
    top_n: Option<TopNSet>,
    count_min_sketch: Option<CountMinSketchSet>,
) -> Result<()> {
    writeln!(file, "expr          : {}", expr_texts.join(", "))?;

    let in_stats = column_stats_to_string(&column_stats);
    let in_top_n = top_n.as_ref().map(top_n_to_string);
    let in_count_min_sketch = count_min_sketch.as_ref().map(count_min_sketch_to_string);
    let mut estimator = SelectivityEstimator::new(column_stats, cardinality);
    if let Some(top_n) = top_n {
        estimator = estimator.with_top_n(top_n);
    }
    if let Some(count_min_sketch) = count_min_sketch {
        estimator = estimator.with_count_min_sketch(count_min_sketch);
    }
    let estimated_rows = estimator.apply(predicates)?;
    let out_stats = estimator.into_column_stats();

    writeln!(
        file,
        "cardinality   : {}",
        cardinality_to_string(cardinality)
    )?;
    writeln!(file, "estimated     : {estimated_rows}")?;
    writeln!(file, "in stats      :\n{in_stats}")?;
    if let Some(in_top_n) = in_top_n {
        writeln!(file, "in topn       :\n{in_top_n}")?;
    }
    if let Some(in_count_min_sketch) = in_count_min_sketch {
        writeln!(file, "in cms        :\n{in_count_min_sketch}")?;
    }
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

fn top_n_to_string(top_n: &TopNSet) -> String {
    let mut keys = top_n.keys().copied().collect::<Vec<_>>();
    keys.sort();

    keys.iter()
        .flat_map(|i| {
            top_n[i].values.iter().map(move |entry| {
                format!(
                    "{i} {:?} => {} (error {})",
                    entry.scalar, entry.count, entry.error
                )
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn count_min_sketch_to_string(count_min_sketch: &CountMinSketchSet) -> String {
    let mut keys = count_min_sketch.keys().copied().collect::<Vec<_>>();
    keys.sort();

    keys.iter()
        .map(|i| format!("{i} present"))
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
    let comparison_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(10),
        max: Datum::UInt(19),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let comparison_columns = &[("a", UInt64Type::data_type())];
    for expr in [
        "a = 5",
        "a = 15",
        "a != 5",
        "a != 15",
        "a > 5",
        "a > 10",
        "a > 16",
        "a > 19",
        "a > 25",
        "a >= 5",
        "a >= 10",
        "a >= 16",
        "a >= 19",
        "a >= 25",
        "a < 5",
        "a < 10",
        "a < 16",
        "a < 19",
        "a < 25",
        "a <= 5",
        "a <= 10",
        "a <= 16",
        "a <= 19",
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
    for expr in ["15 = a", "16 < a", "16 <= a", "16 > a", "16 >= a"] {
        run_case(
            &mut file,
            expr,
            comparison_columns,
            comparison_stats.clone(),
        )?;
    }

    write_case_title(
        &mut file,
        "topn_equality_cache",
        "Equality predicates should use exact TopN frequencies when the constant is cached.",
    )?;
    let top_n_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(999),
        ndv: NdvEstimate::exact(1000.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let top_n = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(42)),
            count: 37,
            error: 0,
        }],
        min_index: None,
    })]);
    let top_n_columns = &[("id", UInt64Type::data_type())];
    for expr in ["id = 42", "id != 42", "id = 7"] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            top_n_stats.clone(),
            StatCardinality::estimate(1000.0),
            Some(top_n.clone()),
            None,
        )?;
    }

    write_case_title(
        &mut file,
        "count_min_sketch_equality_cache",
        "Equality predicates should use Count-Min Sketch estimates when the value is clearly above the NDV fallback.",
    )?;
    let count_min_sketch_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(999),
        ndv: NdvEstimate::exact(100.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let mut column_count_min_sketch = ColumnCountMinSketch::new(4096, 4);
    column_count_min_sketch.add_with_count(Scalar::Number(NumberScalar::UInt64(77)).as_ref(), 42);
    let count_min_sketch =
        CountMinSketchSet::from_iter([(Symbol::new(0), column_count_min_sketch)]);
    for expr in ["id = 77", "id != 77"] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            count_min_sketch_stats.clone(),
            StatCardinality::estimate(1000.0),
            None,
            Some(count_min_sketch.clone()),
        )?;
    }

    write_case_title(
        &mut file,
        "count_min_sketch_equality_cache_fallback",
        "Count-Min Sketch estimates should fall back when the value is not clearly above the NDV fallback.",
    )?;
    let mut coarse_count_min_sketch = ColumnCountMinSketch::new(64, 4);
    coarse_count_min_sketch.add_with_count(Scalar::Number(NumberScalar::UInt64(77)).as_ref(), 42);
    let coarse_count_min_sketch =
        CountMinSketchSet::from_iter([(Symbol::new(0), coarse_count_min_sketch)]);
    for expr in ["id = 77", "id != 77"] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            count_min_sketch_stats.clone(),
            StatCardinality::estimate(1000.0),
            None,
            Some(coarse_count_min_sketch.clone()),
        )?;
    }

    write_case_title(
        &mut file,
        "count_min_sketch_hot_value_with_coarse_error",
        "Count-Min Sketch estimates should still be used when the error bound is coarse but the value is clearly hot.",
    )?;
    let hot_count_min_sketch_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(9999),
        ndv: NdvEstimate::exact(2001.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let mut hot_count_min_sketch = ColumnCountMinSketch::new(2000, 4);
    hot_count_min_sketch.add_with_count(Scalar::Number(NumberScalar::UInt64(0)).as_ref(), 160000);
    let hot_count_min_sketch =
        CountMinSketchSet::from_iter([(Symbol::new(0), hot_count_min_sketch)]);
    for expr in ["id = 0", "id != 0"] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            hot_count_min_sketch_stats.clone(),
            StatCardinality::estimate(200000.0),
            None,
            Some(hot_count_min_sketch.clone()),
        )?;
    }

    write_case_title(
        &mut file,
        "topn_precedes_count_min_sketch",
        "TopN equality estimates should take precedence when both TopN and Count-Min Sketch have a frequency for the scalar.",
    )?;
    let top_n_precedence = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(77)),
            count: 37,
            error: 0,
        }],
        min_index: None,
    })]);
    let mut conflicting_count_min_sketch = ColumnCountMinSketch::new(4096, 4);
    conflicting_count_min_sketch
        .add_with_count(Scalar::Number(NumberScalar::UInt64(77)).as_ref(), 80);
    let conflicting_count_min_sketch =
        CountMinSketchSet::from_iter([(Symbol::new(0), conflicting_count_min_sketch)]);
    let raw_expr = parse_raw_expr("id = 77", top_n_columns, &BUILTIN_FUNCTIONS);
    let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
    run_scalar_case_with_predicates(
        &mut file,
        &["id = 77"],
        &[predicate],
        count_min_sketch_stats.clone(),
        StatCardinality::estimate(1000.0),
        Some(top_n_precedence),
        Some(conflicting_count_min_sketch),
    )?;

    write_case_title(
        &mut file,
        "count_min_sketch_precedes_approximate_topn",
        "Count-Min Sketch should be used when an approximate TopN hit is looser than the CMS estimate.",
    )?;
    let wide_hot_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(99999),
        ndv: NdvEstimate::exact(100000.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let approximate_top_n_hit = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(199)),
            count: 20000,
            error: 17500,
        }],
        min_index: None,
    })]);
    let mut tighter_count_min_sketch = ColumnCountMinSketch::new(4096, 4);
    tighter_count_min_sketch
        .add_with_count(Scalar::Number(NumberScalar::UInt64(199)).as_ref(), 2500);
    let tighter_count_min_sketch =
        CountMinSketchSet::from_iter([(Symbol::new(0), tighter_count_min_sketch)]);
    let raw_expr = parse_raw_expr("id = 199", top_n_columns, &BUILTIN_FUNCTIONS);
    let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
    run_scalar_case_with_predicates(
        &mut file,
        &["id = 199"],
        &[predicate],
        wide_hot_stats,
        StatCardinality::estimate(1000000.0),
        Some(approximate_top_n_hit),
        Some(tighter_count_min_sketch),
    )?;

    write_case_title(
        &mut file,
        "topn_equality_cache_with_and_filters",
        "TopN equality estimates should compose with AND filters.",
    )?;
    let constrained_top_n = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 2,
        values: vec![
            ColumnTopNEntry {
                scalar: Scalar::Number(NumberScalar::UInt64(1)),
                count: 300,
                error: 0,
            },
            ColumnTopNEntry {
                scalar: Scalar::Number(NumberScalar::UInt64(2)),
                count: 200,
                error: 0,
            },
        ],
        min_index: None,
    })]);
    for expr in [
        "and_filters(id = 1, id = 2)",
        "and_filters(id > 10, id = 1)",
    ] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            top_n_stats.clone(),
            StatCardinality::estimate(1000.0),
            Some(constrained_top_n.clone()),
            None,
        )?;
    }

    write_case_title(
        &mut file,
        "topn_equality_cache_with_error",
        "Approximate TopN frequencies should use the count upper bound for equality and the lower bound for inequality.",
    )?;
    let approximate_top_n = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(42)),
            count: 100,
            error: 60,
        }],
        min_index: None,
    })]);
    for expr in ["id = 42", "id != 42"] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            top_n_stats.clone(),
            StatCardinality::estimate(1000.0),
            Some(approximate_top_n.clone()),
            None,
        )?;
    }

    write_case_title(
        &mut file,
        "topn_equality_cache_fallback",
        "Approximate TopN hits should fall back when the lower bound does not exceed the NDV estimate.",
    )?;
    let fallback_top_n_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(999),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    let fallback_top_n = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(42)),
            count: 500,
            error: 450,
        }],
        min_index: None,
    })]);
    for expr in ["id = 42", "id != 42"] {
        let raw_expr = parse_raw_expr(expr, top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            fallback_top_n_stats.clone(),
            StatCardinality::estimate(1000.0),
            Some(fallback_top_n.clone()),
            None,
        )?;
    }

    write_case_title(
        &mut file,
        "topn_nullable_not_equal",
        "TopN inequality estimates should exclude null rows from SQL not-equal matches.",
    )?;
    let nullable_top_n_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(1),
        max: Datum::UInt(9),
        ndv: NdvEstimate::exact(5.0),
        null_count: StatCount::exact(50),
        histogram: None,
    })]);
    let nullable_top_n = TopNSet::from_iter([(Symbol::new(0), ColumnTopN {
        capacity: 1,
        values: vec![ColumnTopNEntry {
            scalar: Scalar::Number(NumberScalar::UInt64(1)),
            count: 10,
            error: 0,
        }],
        min_index: None,
    })]);
    let nullable_top_n_columns = &[("n", UInt64Type::data_type().wrap_nullable())];
    for expr in ["n = 1", "n != 1"] {
        let raw_expr = parse_raw_expr(expr, nullable_top_n_columns, &BUILTIN_FUNCTIONS);
        let predicate = raw_expr_to_scalar(&raw_expr, nullable_top_n_columns);
        run_scalar_case_with_predicates(
            &mut file,
            &[expr],
            &[predicate],
            nullable_top_n_stats.clone(),
            StatCardinality::estimate(100.0),
            Some(nullable_top_n.clone()),
            None,
        )?;
    }

    Ok(())
}

#[test]
fn test_selectivity_typed_comparison_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity_typed_comparison.txt")?;
    write_case_title(
        &mut file,
        "typed_comparison_predicates",
        "Typed comparison predicates should respect integer boundaries and nullable inputs.",
    )?;
    let int_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(1),
        max: Datum::Int(10),
        ndv: NdvEstimate::exact(10.0),
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
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(30),
        histogram: None,
    })]);
    run_case(
        &mut file,
        "n > 5",
        &[("n", Int64Type::data_type().wrap_nullable())],
        nullable_stats,
    )?;

    write_case_title(
        &mut file,
        "typed_constant_compatibility",
        "Constant constraints should apply only when the typed comparison is compatible.",
    )?;
    let date_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(20),
        max: Datum::Int(29),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in ["d = cast(10 as date)", "d = cast(25 as date)"] {
        run_case(
            &mut file,
            expr,
            &[("d", DataType::Date)],
            date_stats.clone(),
        )?;
    }
    let number_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(1),
        max: Datum::Int(10),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    run_case(
        &mut file,
        "number = '5'",
        &[("number", DataType::Number(NumberDataType::Int32))],
        number_stats.clone(),
    )?;
    let typed_number_predicate = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: ComparisonOp::Equal.to_func_name().to_string(),
        params: vec![],
        arguments: vec![
            ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "number".to_string(),
                    Symbol::new(0),
                    Box::new(DataType::Number(NumberDataType::Int32)),
                    Visibility::Visible,
                )
                .build(),
            }),
            ScalarExpr::TypedConstantExpr(
                ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt64(5)),
                },
                DataType::Number(NumberDataType::UInt64),
            ),
        ],
    });
    run_scalar_case_with_predicates(
        &mut file,
        &["number = UInt64(5)"],
        &[typed_number_predicate],
        number_stats.clone(),
        StatCardinality::estimate(100.0),
        None,
        None,
    )?;
    let decimal_size = DecimalSize::new(10, 2).unwrap();
    let decimal_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Float(1.0.into()),
        max: Datum::Float(4.0.into()),
        ndv: NdvEstimate::exact(4.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in [
        "dec = cast(5.00 as decimal(10, 2))",
        "dec = cast(2.00 as decimal(10, 2))",
    ] {
        run_case(
            &mut file,
            expr,
            &[("dec", DataType::Decimal(decimal_size))],
            decimal_stats.clone(),
        )?;
    }

    write_case_title(
        &mut file,
        "string_comparison_predicates",
        "String equality should use domain and function statistics without assuming unbounded strings are impossible.",
    )?;
    let string_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Bytes(b"b".to_vec()),
        max: Datum::Bytes(b"e".to_vec()),
        ndv: NdvEstimate::exact(4.0),
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

    write_case_title(
        &mut file,
        "boolean_comparison_predicates",
        "Boolean comparison predicates should keep constrained bounds and NDV consistent.",
    )?;
    let bool_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Bool(false),
        max: Datum::Bool(true),
        ndv: NdvEstimate::exact(2.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    run_case(
        &mut file,
        "flag = true",
        &[("flag", BooleanType::data_type())],
        bool_stats.clone(),
    )?;
    run_case(
        &mut file,
        "flag > false",
        &[("flag", BooleanType::data_type())],
        bool_stats,
    )?;

    Ok(())
}

#[test]
fn test_selectivity_histogram_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity_histogram.txt")?;
    write_case_title(
        &mut file,
        "histogram_comparison_predicates",
        "Histogram comparisons should restrict bucket ranges, counts, and accuracy consistently.",
    )?;
    let edge_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(1),
        max: Datum::Int(10),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::Int(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(1, 10, 100.0, 10.0)],
            avg_spacing: None,
        })),
    })]);
    for expr in ["h >= 10", "h < 10", "h != 5"] {
        run_case(
            &mut file,
            expr,
            &[("h", Int64Type::data_type())],
            edge_histogram_stats.clone(),
        )?;
    }
    let uint8_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(9),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 9, 100.0, 10.0)],
            avg_spacing: None,
        })),
    })]);
    run_case(
        &mut file,
        "u > 4",
        &[("u", UInt8Type::data_type())],
        uint8_histogram_stats,
    )?;
    let multi_bucket_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(19),
        ndv: NdvEstimate::exact(20.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(0, 9, 50.0, 10.0),
                TypedHistogramBucket::new(10, 19, 50.0, 10.0),
            ],
            avg_spacing: None,
        })),
    })]);
    for expr in ["m >= 5", "m < 15"] {
        run_case(
            &mut file,
            expr,
            &[("m", UInt64Type::data_type())],
            multi_bucket_histogram_stats.clone(),
        )?;
    }
    let skewed_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(9),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(0, 4, 50.0, 5.0),
                TypedHistogramBucket::new(5, 9, 5.0, 5.0),
            ],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["s >= 2"],
        &[("s", UInt64Type::data_type())],
        skewed_histogram_stats,
        StatCardinality::estimate(55.0),
    )?;
    let nullable_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(9),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(30),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 9, 70.0, 10.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["n >= 5"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        nullable_histogram_stats,
        StatCardinality::estimate(100.0),
    )?;
    let tail_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(737),
        ndv: NdvEstimate::exact(738.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 737, 738.0, 738.0)],
            avg_spacing: None,
        })),
    })]);
    for expr in ["tail > 731", "tail > 700", "tail > 737"] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[("tail", UInt64Type::data_type())],
            tail_histogram_stats.clone(),
            StatCardinality::estimate(738.0),
        )?;
    }

    write_case_title(
        &mut file,
        "histogram_multi_step_propagation",
        "Partial numeric buckets should keep row-mass alignment and accumulated range constraints visible across AND predicates.",
    )?;
    let float_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Float(F64::from(0.0)),
        max: Datum::Float(F64::from(20.0)),
        ndv: NdvEstimate::exact(20.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::Float(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(F64::from(0.0), F64::from(10.0), 100.0, 10.0),
                TypedHistogramBucket::new(F64::from(10.0), F64::from(20.0), 100.0, 10.0),
            ],
            avg_spacing: None,
        })),
    })]);
    let float_column = || {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "f".to_string(),
                Symbol::new(0),
                Box::new(DataType::Number(NumberDataType::Float64)),
                Visibility::Visible,
            )
            .build(),
        })
    };
    let float_constant = |value| {
        ScalarExpr::TypedConstantExpr(
            ConstantExpr {
                span: None,
                value: Scalar::Number(NumberScalar::Float64(F64::from(value))),
            },
            DataType::Number(NumberDataType::Float64),
        )
    };
    let float_predicate = |op: ComparisonOp, value| {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: op.to_func_name().to_string(),
            params: vec![],
            arguments: vec![float_column(), float_constant(value)],
        })
    };
    let float_gte_15 = float_predicate(ComparisonOp::GTE, 15.0);
    let float_lte_19 = float_predicate(ComparisonOp::LTE, 19.0);
    run_scalar_case_with_predicates(
        &mut file,
        &["f >= Float64(15)"],
        std::slice::from_ref(&float_gte_15),
        float_histogram_stats.clone(),
        StatCardinality::estimate(200.0),
        None,
        None,
    )?;
    run_scalar_case_with_predicates(
        &mut file,
        &["f >= Float64(15)", "f <= Float64(19)"],
        &[float_gte_15, float_lte_19],
        float_histogram_stats,
        StatCardinality::estimate(200.0),
        None,
        None,
    )?;

    write_case_title(
        &mut file,
        "histogram_row_count_mismatch",
        "Histograms with row counts above the non-null cardinality should scale output row mass without trusting bucket distincts as exact.",
    )?;
    let row_count_mismatch_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(9),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(50),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 9, 100.0, 10.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["mismatch > 3"],
        &[("mismatch", UInt64Type::data_type().wrap_nullable())],
        row_count_mismatch_stats,
        StatCardinality::estimate(100.0),
    )?;

    write_case_title(
        &mut file,
        "distorted_histogram_ranges",
        "Distorted histograms should still record precise range constraints while using the lower-bound selectivity fallback.",
    )?;
    let distorted_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(1000),
        ndv: NdvEstimate::exact(100.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::Float(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                F64::from(0.0),
                F64::from(1000.0),
                100.0,
                100.0,
            )],
            avg_spacing: Some(1e13),
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["d >= 500"],
        &[("d", UInt64Type::data_type())],
        distorted_histogram_stats,
        StatCardinality::estimate(100.0),
    )?;

    write_case_title(
        &mut file,
        "histogram_accuracy_provenance",
        "Whole-bucket range pruning may trust ANALYZE distinct counts, but derived bucket distinct values are only estimates.",
    )?;
    let analyzed_string_histogram_stats =
        ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
            min: Datum::Bytes(b"a".to_vec()),
            max: Datum::Bytes(b"z".to_vec()),
            ndv: NdvEstimate::exact(26.0),
            null_count: StatCount::exact(0),
            histogram: Some(Histogram::Bytes(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![
                    TypedHistogramBucket::new(b"a".to_vec(), b"f".to_vec(), 60.0, 6.0),
                    TypedHistogramBucket::new(b"m".to_vec(), b"z".to_vec(), 40.0, 20.0),
                ],
                avg_spacing: None,
            })),
        })]);
    run_case_with_predicates(
        &mut file,
        &["s >= 'm'"],
        &[("s", DataType::String)],
        analyzed_string_histogram_stats.clone(),
        StatCardinality::estimate(100.0),
    )?;
    // A previous independent filter may scale the analyzed histogram without
    // observing which values survived. The [m,z] bucket's derived distinct count
    // is 10, but a real filtered input can still keep all 20 values in that range.
    let derived_string_histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Bytes(b"a".to_vec()),
        max: Datum::Bytes(b"z".to_vec()),
        ndv: NdvEstimate::new(13.0, 26.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::Bytes(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(b"a".to_vec(), b"f".to_vec(), 30.0, 3.0),
                TypedHistogramBucket::new(b"m".to_vec(), b"z".to_vec(), 20.0, 10.0),
            ],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["s >= 'm'"],
        &[("s", DataType::String)],
        derived_string_histogram_stats,
        StatCardinality::estimate(50.0),
    )?;
    run_case_with_predicates(
        &mut file,
        &["s > 'p'"],
        &[("s", DataType::String)],
        analyzed_string_histogram_stats,
        StatCardinality::estimate(100.0),
    )?;

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
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(10),
            histogram: None,
        }),
    ]);
    for expr in [
        "and_filters(a = 5, a > 3)",
        "and_filters(a != 5, a = 5)",
        "and_filters(a = 5, a != 5)",
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
    run_case(
        &mut file,
        "is_not_null(a + 1)",
        &[("a", UInt64Type::data_type())],
        logical_stats.clone(),
    )?;
    for expr in ["a", "cast(a as uint64)"] {
        run_case(
            &mut file,
            expr,
            &[("a", UInt64Type::data_type())],
            logical_stats.clone(),
        )?;
    }
    run_case_with_predicates(
        &mut file,
        &["a > 3", "a < 6"],
        &[("a", UInt64Type::data_type())],
        logical_stats.clone(),
        StatCardinality::estimate(100.0),
    )?;
    run_case_with_predicates(
        &mut file,
        &["a != 5", "a >= 5", "a <= 5"],
        &[("a", UInt64Type::data_type())],
        logical_stats.clone(),
        StatCardinality::estimate(100.0),
    )?;

    write_case_title(
        &mut file,
        "histogram_logical_predicates",
        "AND constraints should be visible to later predicates, while OR and NOT should only affect final selectivity.",
    )?;
    let histogram_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(9),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 9, 100.0, 10.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["a > 3", "a > 4"],
        &[("a", UInt64Type::data_type())],
        histogram_stats.clone(),
        StatCardinality::estimate(100.0),
    )?;
    for expr in ["or_filters(a > 3, a > 4)", "not(a > 3)"] {
        run_case(
            &mut file,
            expr,
            &[("a", UInt64Type::data_type())],
            histogram_stats.clone(),
        )?;
    }
    run_case(
        &mut file,
        "or_filters(missing = 1, a = 5)",
        &[
            ("a", UInt64Type::data_type()),
            ("missing", UInt64Type::data_type()),
        ],
        histogram_stats.clone(),
    )?;
    run_case(
        &mut file,
        "missing = 1",
        &[
            ("a", UInt64Type::data_type()),
            ("missing", UInt64Type::data_type()),
        ],
        histogram_stats.clone(),
    )?;
    run_case(
        &mut file,
        "and_filters(missing = 1, true)",
        &[
            ("a", UInt64Type::data_type()),
            ("missing", UInt64Type::data_type()),
        ],
        histogram_stats.clone(),
    )?;
    let combined_histogram_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: Some(Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(0, 9, 100.0, 10.0)],
                avg_spacing: None,
            })),
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: Some(Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(0, 9, 100.0, 10.0)],
                avg_spacing: None,
            })),
        }),
    ]);
    run_case_with_predicates(
        &mut file,
        &["a >= 5", "b >= 8"],
        &[
            ("a", UInt64Type::data_type()),
            ("b", UInt64Type::data_type()),
        ],
        combined_histogram_stats,
        StatCardinality::estimate(100.0),
    )?;

    write_case_title(
        &mut file,
        "constant_folded_distribution_predicates",
        "Constant folding should compose with arithmetic distribution predicates and visible AND constraints.",
    )?;
    let distribution_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: Some(Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(0, 9, 100.0, 10.0)],
                avg_spacing: None,
            })),
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(4),
            ndv: NdvEstimate::exact(5.0),
            null_count: StatCount::exact(0),
            histogram: Some(Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(0, 4, 50.0, 5.0)],
                avg_spacing: None,
            })),
        }),
    ]);
    run_case_with_predicates(
        &mut file,
        &["a > 3", "a + 1 > 6", "1 + 1 = 2"],
        &[
            ("a", UInt64Type::data_type()),
            ("b", UInt64Type::data_type()),
        ],
        distribution_stats.clone(),
        StatCardinality::estimate(100.0),
    )?;
    run_case(
        &mut file,
        "or_filters(and_filters(a > 7, 1 = 0), a + 1 > 6)",
        &[
            ("a", UInt64Type::data_type()),
            ("b", UInt64Type::data_type()),
        ],
        distribution_stats.clone(),
    )?;
    run_case(
        &mut file,
        "and_filters(a > 4, a + b > 10)",
        &[
            ("a", UInt64Type::data_type()),
            ("b", UInt64Type::data_type()),
        ],
        distribution_stats,
    )?;

    write_case_title(
        &mut file,
        "constant_logical_predicates",
        "Constant predicates should preserve Zero and All through logical composition.",
    )?;
    for expr in [
        "1",
        "0",
        "-1",
        "true",
        "false",
        "null",
        "'not-a-number'",
        "not(false)",
        "not(true)",
    ] {
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
        ndv: NdvEstimate::exact(4.0),
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
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(0),
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(2), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(4),
            ndv: NdvEstimate::exact(5.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(3), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(4),
            ndv: NdvEstimate::exact(5.0),
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
        StatCardinality::estimate(10.0),
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
        ndv: NdvEstimate::exact(6.0),
        null_count: StatCount::estimate(8.0, 8.0),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 5, 6.0, 6.0)],
            avg_spacing: None,
        })),
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
            ndv: NdvEstimate::exact(6.0),
            null_count: StatCount::exact(2),
            histogram: Some(Histogram::UInt(TypedHistogram {
                accuracy: false,
                row_scale: 1.0,
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
    let unsatisfiable_range_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(5),
        ndv: NdvEstimate::exact(6.0),
        null_count: StatCount::exact(2),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 5, 6.0, 6.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["n > 10"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        unsatisfiable_range_stats,
        StatCardinality::estimate(8.0),
    )?;
    let exact_zero_cardinality_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(5),
        ndv: NdvEstimate::exact(6.0),
        null_count: StatCount::exact(2),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 5, 6.0, 6.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["n > 1"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        exact_zero_cardinality_stats,
        StatCardinality::exact(0),
    )?;

    write_case_title(
        &mut file,
        "exact_null_predicates",
        "Exact all-null input domains should fold is_not_null to an empty result and clear distributions.",
    )?;
    let exact_all_null_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(5),
        ndv: NdvEstimate::exact(6.0),
        null_count: StatCount::exact(8),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
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
    let exact_nullable_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::UInt(0),
        max: Datum::UInt(5),
        ndv: NdvEstimate::exact(6.0),
        null_count: StatCount::exact(2),
        histogram: Some(Histogram::UInt(TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 5, 6.0, 6.0)],
            avg_spacing: None,
        })),
    })]);
    run_case_with_predicates(
        &mut file,
        &["is_not_null(n)"],
        &[("n", UInt64Type::data_type().wrap_nullable())],
        exact_nullable_stats,
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
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: NdvEstimate::exact(10.0),
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
    run_case_with_predicates(
        &mut file,
        &["a % 0 = 0"],
        &[("a", UInt64Type::data_type())],
        mod_stats,
        StatCardinality::estimate(100.0),
    )?;
    let signed_mod_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(-9),
        max: Datum::Int(9),
        ndv: NdvEstimate::exact(19.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in ["i % 4 = -1", "i % 4 = 5", "i % -4 = -1", "i % -4 = 5"] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[("i", Int64Type::data_type())],
            signed_mod_stats.clone(),
            StatCardinality::estimate(100.0),
        )?;
    }
    let signed_min_mod_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Int(i64::MIN),
        max: Datum::Int(9),
        ndv: NdvEstimate::exact(10.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    run_case_with_predicates(
        &mut file,
        &["i % -1 = 0"],
        &[("i", Int64Type::data_type())],
        signed_min_mod_stats,
        StatCardinality::estimate(100.0),
    )?;

    write_case_title(
        &mut file,
        "like_predicates",
        "LIKE predicates should use string-domain statistics when estimating selectivity.",
    )?;
    let like_stats = ColumnStatSet::from_iter([(Symbol::new(0), ColumnStat {
        min: Datum::Bytes("aa".as_bytes().to_vec()),
        max: Datum::Bytes("zz".as_bytes().to_vec()),
        ndv: NdvEstimate::exact(40.0),
        null_count: StatCount::exact(0),
        histogram: None,
    })]);
    for expr in [
        "s like 'ab%'",
        "s like '%ab_'",
        "s like 'a\\_b'",
        "s like '%%%%'",
    ] {
        run_case_with_predicates(
            &mut file,
            &[expr],
            &[("s", DataType::String)],
            like_stats.clone(),
            StatCardinality::estimate(100.0),
        )?;
    }
    let dynamic_like_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::Bytes("aa".as_bytes().to_vec()),
            max: Datum::Bytes("zz".as_bytes().to_vec()),
            ndv: NdvEstimate::exact(40.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::Bytes("a%".as_bytes().to_vec()),
            max: Datum::Bytes("z%".as_bytes().to_vec()),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        }),
    ]);
    run_case_with_predicates(
        &mut file,
        &["s like p"],
        &[("s", DataType::String), ("p", DataType::String)],
        dynamic_like_stats,
        StatCardinality::estimate(100.0),
    )?;

    Ok(())
}
