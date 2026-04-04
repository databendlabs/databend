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
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::ColumnStat;
use databend_common_sql::optimizer::ir::ColumnStatSet;
use databend_common_sql::optimizer::ir::Ndv;
use databend_common_sql::optimizer::ir::SelectivityEstimator;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::CastExpr;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql_test_support::parse_raw_expr;
use databend_common_statistics::Datum;

use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

fn run_case(
    file: &mut impl Write,
    expr_text: &str,
    columns: &[(&str, DataType)],
    column_stats: ColumnStatSet,
) -> Result<()> {
    writeln!(file, "expr          : {expr_text}")?;

    let in_stats = column_stats_to_string(&column_stats);
    let raw_expr = parse_raw_expr(expr_text, columns, &BUILTIN_FUNCTIONS);
    let expr = raw_expr_to_scalar(&raw_expr, columns);
    let cardinality = 100.0;
    let mut estimator = SelectivityEstimator::new(column_stats, cardinality);
    let estimated_rows = estimator.apply(&[expr])?;
    let out_stats = estimator.column_stats();

    writeln!(file, "cardinality   : {cardinality}")?;
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
fn test_selectivity_estimator_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "selectivity.txt")?;

    write_case_title(
        &mut file,
        "comparison_predicates",
        "Comparison predicates should update estimated rows and column stats consistently.",
    )?;
    let comparison_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(10),
            max: Datum::UInt(20),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(10),
            max: Datum::UInt(20),
            ndv: Ndv::Stat(10.0),
            null_count: 10,
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
        "logical_predicates",
        "Logical predicate composition should combine selectivity estimates and null handling.",
    )?;
    let logical_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: Ndv::Stat(10.0),
            null_count: 10,
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

    write_case_title(
        &mut file,
        "modulo_predicates",
        "Modulo predicates should narrow value ranges only when the comparison is satisfiable.",
    )?;
    let mod_stats = ColumnStatSet::from_iter([
        (Symbol::new(0), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: Ndv::Stat(10.0),
            null_count: 0,
            histogram: None,
        }),
        (Symbol::new(1), ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(9),
            ndv: Ndv::Stat(10.0),
            null_count: 10,
            histogram: None,
        }),
    ]);
    for expr in ["a % 4 = 1", "a % 4 = 5"] {
        run_case(
            &mut file,
            expr,
            &[("a", UInt64Type::data_type())],
            mod_stats.clone(),
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
        ndv: Ndv::Stat(52.0),
        null_count: 0,
        histogram: None,
    })]);
    for expr in ["s like 'ab%'", "s like '%ab_'"] {
        run_case(
            &mut file,
            expr,
            &[("s", DataType::String)],
            like_stats.clone(),
        )?;
    }

    Ok(())
}
