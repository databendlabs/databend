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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::ColumnRef as ExprColumnRef;
use databend_common_expression::Expr;
use databend_common_expression::RawExpr;
use databend_common_expression::Symbol;
use databend_common_expression::type_check;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::optimizers::rule::Rule;
use databend_common_sql::optimizer::optimizers::rule::RuleNormalizeScalarFilter;
use databend_common_sql::optimizer::optimizers::rule::TransformResult;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::CastExpr;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::DummyTableScan;
use databend_common_sql::plans::Filter;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Scan;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;
use crate::framework::golden::write_case_title;

#[derive(Clone, Copy)]
enum Target {
    Filter,
    Scan,
}

impl Target {
    fn as_str(self) -> &'static str {
        match self {
            Target::Filter => "filter",
            Target::Scan => "scan",
        }
    }
}

struct Case {
    name: &'static str,
    description: &'static str,
    target: Target,
    expr_text: &'static str,
}

fn raw_expr_to_scalar(raw_expr: &RawExpr, columns: &[(&str, DataType)]) -> ScalarExpr {
    raw_expr_to_typed_scalar(raw_expr, columns).0
}

fn raw_expr_to_typed_scalar(
    raw_expr: &RawExpr,
    columns: &[(&str, DataType)],
) -> (ScalarExpr, Expr) {
    match raw_expr {
        RawExpr::Constant { scalar, .. } => (
            ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar.clone(),
            }),
            type_check::check(raw_expr, &BUILTIN_FUNCTIONS).unwrap(),
        ),
        RawExpr::ColumnRef { span, id, .. } => {
            let index = *id;
            let (name, data_type) = &columns[index];
            let column = ColumnBindingBuilder::new(
                name.to_string(),
                Symbol::from_field_index(index),
                Box::new(data_type.clone()),
                Visibility::Visible,
            )
            .build();
            (
                ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
                Expr::ColumnRef(ExprColumnRef {
                    span: *span,
                    id: index,
                    data_type: data_type.clone(),
                    display_name: name.to_string(),
                }),
            )
        }
        RawExpr::Cast {
            span,
            expr,
            dest_type,
            is_try,
            ..
        } => {
            let (scalar, typed_expr) = raw_expr_to_typed_scalar(expr, columns);
            let typed_expr =
                type_check::check_cast(*span, *is_try, typed_expr, dest_type, &BUILTIN_FUNCTIONS)
                    .unwrap();
            (
                ScalarExpr::CastExpr(CastExpr {
                    span: None,
                    is_try: *is_try,
                    argument: Box::new(scalar),
                    target_type: Box::new(dest_type.clone()),
                }),
                typed_expr,
            )
        }
        RawExpr::FunctionCall {
            span,
            name,
            args,
            params,
        } => {
            let (arguments, typed_arguments): (Vec<_>, Vec<_>) = args
                .iter()
                .map(|arg| raw_expr_to_typed_scalar(arg, columns))
                .unzip();
            let typed_expr = type_check::check_function(
                *span,
                name,
                params,
                &typed_arguments,
                &BUILTIN_FUNCTIONS,
            )
            .unwrap();
            let return_type = typed_expr.data_type().clone();
            (
                ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: name.clone(),
                    params: params.clone(),
                    arguments,
                    return_type: Box::new(return_type),
                }),
                typed_expr,
            )
        }
        RawExpr::LambdaFunctionCall { .. } => {
            unreachable!("lambda expressions are not used in tests")
        }
    }
}

fn build_input(target: Target, predicate: ScalarExpr) -> SExpr {
    match target {
        Target::Filter => SExpr::create_unary(
            Filter {
                predicates: vec![predicate],
            },
            SExpr::create_leaf(DummyTableScan::default()),
        ),
        Target::Scan => SExpr::create_leaf(Scan {
            push_down_predicates: Some(vec![predicate]),
            ..Default::default()
        }),
    }
}

fn predicate_texts(target: Target, expr: &SExpr) -> Result<Vec<String>> {
    let predicates = match target {
        Target::Filter => expr.plan.as_filter().unwrap().predicates.as_slice(),
        Target::Scan => expr
            .plan
            .as_scan()
            .unwrap()
            .push_down_predicates
            .as_deref()
            .unwrap_or(&[]),
    };

    predicates
        .iter()
        .map(|predicate| Ok(predicate.as_expr()?.to_string()))
        .collect()
}

fn write_case(file: &mut impl Write, case: &Case, columns: &[(&str, DataType)]) -> Result<()> {
    write_case_title(file, case.name, case.description)?;
    writeln!(file, "operator: {}", case.target.as_str())?;
    writeln!(file, "input: {}", case.expr_text)?;

    let raw_expr = parse_raw_expr(case.expr_text, columns, &BUILTIN_FUNCTIONS);
    let predicate = raw_expr_to_scalar(&raw_expr, columns);
    let input = build_input(case.target, predicate);

    let rule = RuleNormalizeScalarFilter::new();
    let mut state = TransformResult::new();
    rule.apply(&input, &mut state)?;

    let (changed, output) = match state.results().first() {
        Some(result) => (true, result),
        None => (false, &input),
    };

    writeln!(file, "changed: {changed}")?;
    for (index, predicate) in predicate_texts(case.target, output)?.iter().enumerate() {
        writeln!(file, "output[{index}]: {predicate}")?;
    }
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_normalize_scalar_filter_rule_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "normalize_scalar.txt")?;

    let columns = &[
        ("a", UInt64Type::data_type()),
        ("b", BooleanType::data_type()),
        ("c", UInt64Type::data_type().wrap_nullable()),
    ];

    let cases = [
        Case {
            name: "filter_keeps_simple_comparison",
            description: "A single comparison should remain unchanged.",
            target: Target::Filter,
            expr_text: "a = 5",
        },
        Case {
            name: "filter_flattens_and_chain",
            description: "Nested AND expressions should normalize into and_filters.",
            target: Target::Filter,
            expr_text: "a != 3 and a != 4 and a != 5",
        },
        Case {
            name: "filter_drops_true_from_and_chain",
            description: "AND normalization should remove no-op true operands.",
            target: Target::Filter,
            expr_text: "a != 3 and true and a != 5",
        },
        Case {
            name: "filter_short_circuits_false_in_and_chain",
            description: "A falsy operand should collapse the whole AND expression to false.",
            target: Target::Filter,
            expr_text: "a != 3 and false and a != 5",
        },
        Case {
            name: "filter_collapses_all_true_and_chain",
            description: "An AND chain of only true operands should collapse to true.",
            target: Target::Filter,
            expr_text: "true and true",
        },
        Case {
            name: "filter_flattens_or_chain",
            description: "Nested OR expressions should normalize into or_filters.",
            target: Target::Filter,
            expr_text: "a = 3 or a = 4 or a = 5",
        },
        Case {
            name: "filter_short_circuits_true_in_or_chain",
            description: "A truthy operand should collapse the whole OR expression to true.",
            target: Target::Filter,
            expr_text: "a = 3 or true or a = 5",
        },
        Case {
            name: "filter_drops_false_from_or_chain",
            description: "OR normalization should remove no-op false operands.",
            target: Target::Filter,
            expr_text: "a = 3 or false or a = 5",
        },
        Case {
            name: "filter_collapses_single_or_operand",
            description: "If only one OR operand remains, normalization should return it directly.",
            target: Target::Filter,
            expr_text: "a = 3 or false",
        },
        Case {
            name: "filter_collapses_all_false_or_chain",
            description: "An OR chain of only false operands should collapse to false.",
            target: Target::Filter,
            expr_text: "false or false",
        },
        Case {
            name: "filter_preserves_nested_or_groups_under_and",
            description: "AND normalization should preserve meaningful OR groupings.",
            target: Target::Filter,
            expr_text: "(a = 9 or a = 8) and (a = 7 or a = 5) and a = 3",
        },
        Case {
            name: "filter_eliminates_double_negation",
            description: "Double negation should simplify to the original predicate.",
            target: Target::Filter,
            expr_text: "not(not(b))",
        },
        Case {
            name: "filter_keeps_non_target_boolean_structure",
            description: "Expressions outside the rule's target patterns should remain intact.",
            target: Target::Filter,
            expr_text: "is_not_null(c < 3 and c < 4)",
        },
        Case {
            name: "scan_normalizes_pushdown_and_chain",
            description: "The same normalization should apply when predicates live on a scan node.",
            target: Target::Scan,
            expr_text: "a != 3 and true and a != 5",
        },
        Case {
            name: "scan_keeps_simple_comparison",
            description: "A scan predicate outside the target patterns should remain unchanged.",
            target: Target::Scan,
            expr_text: "a = 5",
        },
        Case {
            name: "scan_short_circuits_pushdown_or_chain",
            description: "Scan pushdown predicates should also short-circuit truthy OR operands.",
            target: Target::Scan,
            expr_text: "a = 3 or true or a = 5",
        },
    ];

    for case in &cases {
        write_case(&mut file, case, columns)?;
    }

    write_infer_filter_cases(&mut file).await?;
    Ok(())
}

// Exercise constraint normalization through SQL and the complete optimizer pipeline,
// in addition to the local boolean normalization cases above.
async fn write_infer_filter_cases(file: &mut impl Write) -> Result<()> {
    let setup_sqls = &[
        "CREATE TABLE infer_bounds(x BIGINT, y INT, f FLOAT32, d DECIMAL(18, 2), d38 DECIMAL(38, 2), d76 DECIMAL(76, 2), e DECIMAL(38, 3))",
        "CREATE TABLE infer_right(x BIGINT)",
        "CREATE TABLE infer_decimal_chain(a DECIMAL(37,0), b DECIMAL(1,0), c DECIMAL(38,2))",
    ];
    let cases = [
        (
            "redundant_upper_1",
            "Only the tightest upper bound remains.",
            "SELECT x FROM infer_bounds WHERE x > 0 AND x < 10 AND x < 5",
        ),
        (
            "redundant_upper_2",
            "Reordering bounds must not affect the optimized filter.",
            "SELECT x FROM infer_bounds WHERE x < 10 AND x < 5 AND x > 0",
        ),
        (
            "redundant_upper_3",
            "Reordering bounds must not affect the optimized filter.",
            "SELECT x FROM infer_bounds WHERE x < 5 AND x > 0 AND x < 10",
        ),
        (
            "contradiction_1",
            "The tightest bounds contradict, even with an earlier weaker bound.",
            "SELECT x FROM infer_bounds WHERE x > 0 AND x < 5 AND x >= 5",
        ),
        (
            "contradiction_2",
            "Reordering contradictory bounds still eliminates the scan.",
            "SELECT x FROM infer_bounds WHERE x < 5 AND x >= 5 AND x > 0",
        ),
        (
            "contradiction_3",
            "Reordering contradictory bounds still eliminates the scan.",
            "SELECT x FROM infer_bounds WHERE x >= 5 AND x > 0 AND x < 5",
        ),
        (
            "excluded_endpoints",
            "Endpoint exclusions open both bounds; irrelevant and duplicate exclusions disappear.",
            "SELECT x FROM infer_bounds WHERE x >= 0 AND x <= 10 AND x != 0 AND x != 10 AND x != 5 AND x != 5 AND x != 20",
        ),
        (
            "excluded_singleton",
            "Closed coincident bounds plus an exclusion are contradictory.",
            "SELECT x FROM infer_bounds WHERE x != 5 AND x >= 5 AND x <= 5",
        ),
        (
            "equality",
            "An equality absorbs compatible bounds and exclusions.",
            "SELECT x FROM infer_bounds WHERE x > 0 AND x < 10 AND x != 1 AND x = 5",
        ),
        (
            "equivalent_columns",
            "Contradictory bounds are merged across a lossless Int32/Int64 equality.",
            "SELECT x FROM infer_bounds WHERE x = y AND x > 0 AND x < 5 AND y >= 5",
        ),
        (
            "join_and_aggregate",
            "A propagated contradiction eliminates the join inputs but preserves COUNT on empty input.",
            "SELECT count(*) FROM infer_bounds AS l JOIN infer_right AS r ON l.x = r.x WHERE l.x > 0 AND l.x < 5 AND r.x >= 5",
        ),
        (
            "nullable_singleton",
            "Collapsing bounds to equality must still reject NULL rows.",
            "SELECT x FROM infer_bounds WHERE x >= 5 AND x <= 5",
        ),
        (
            "null_comparison",
            "Comparison with NULL cannot be true in a WHERE clause.",
            "SELECT x FROM infer_bounds WHERE x > 0 AND x != NULL",
        ),
        (
            "null_typed_equivalence_bridge",
            "A NULL-typed expression cannot connect exact and floating numeric equivalence classes.",
            "SELECT x FROM infer_bounds CROSS JOIN (SELECT NULL AS n) AS nulls WHERE x = nulls.n AND nulls.n = f AND x > 0 AND f > 0",
        ),
        (
            "nullable_reflexive",
            "Reflexive equality is retained to reject NULL rows.",
            "SELECT x FROM infer_bounds WHERE x = x",
        ),
        (
            "lossless_cast",
            "Integer widening allows constraint inference.",
            "SELECT y FROM infer_bounds WHERE CAST(y AS BIGINT) > 0 AND y < 5 AND y >= 5",
        ),
        (
            "lossy_integer_float_cast",
            "Integer-to-float equality must not propagate a rounded constant to the integer column.",
            "SELECT x FROM infer_bounds WHERE CAST(x AS FLOAT64) = 9007199254740992::FLOAT64 AND x = 9007199254740993",
        ),
        (
            "mixed_integer_float_equality",
            "Equal widened Float64 values must not make different exact integer constants contradictory.",
            "SELECT l.x FROM infer_bounds AS l JOIN infer_right AS r ON CAST(l.x AS FLOAT64) = CAST(r.x AS FLOAT64) WHERE l.x = 9007199254740993 AND r.x = 9007199254740992",
        ),
        (
            "float32_narrowing",
            "A Float64 endpoint that is not exactly Float32 must not be rounded while removing a cast.",
            "SELECT f FROM infer_bounds WHERE CAST(f AS FLOAT64) > 0.1::FLOAT64 AND CAST(f AS FLOAT64) < 0.100000002::FLOAT64",
        ),
        (
            "float_signed_zero",
            "Signed zeros compare equal; excluding that singleton is contradictory.",
            "SELECT f FROM infer_bounds WHERE f >= (-0.0)::FLOAT32 AND f <= 0.0::FLOAT32 AND f != 0.0::FLOAT32",
        ),
        (
            "float_nonfinite",
            "Non-finite constants retain their SQL comparison semantics.",
            "SELECT f FROM infer_bounds WHERE f > '-inf'::FLOAT32 AND f < 'nan'::FLOAT32",
        ),
        (
            "decimal64",
            "Exact Decimal64 bounds collapse to equality.",
            "SELECT d FROM infer_bounds WHERE d >= 9999999999999999.99::DECIMAL(18,2) AND d <= 9999999999999999.99::DECIMAL(18,2)",
        ),
        (
            "decimal128",
            "An excluded Decimal128 boundary is contradictory without floating-point conversion.",
            "SELECT d38 FROM infer_bounds WHERE d38 >= 999999999999999999999999999999999999.99::DECIMAL(38,2) AND d38 <= 999999999999999999999999999999999999.99::DECIMAL(38,2) AND d38 != 999999999999999999999999999999999999.99::DECIMAL(38,2)",
        ),
        (
            "decimal256",
            "An excluded Decimal256 singleton is contradictory at full precision.",
            "SELECT d76 FROM infer_bounds WHERE d76 >= CAST(CONCAT(REPEAT('9',74),'.99') AS DECIMAL(76,2)) AND d76 <= CAST(CONCAT(REPEAT('9',74),'.99') AS DECIMAL(76,2)) AND d76 != CAST(CONCAT(REPEAT('9',74),'.99') AS DECIMAL(76,2))",
        ),
        (
            "decimal_equivalent_scales",
            "Exact comparison across losslessly comparable Decimal types catches propagated contradictions.",
            "SELECT d FROM infer_bounds WHERE d = e AND d < 5.00::DECIMAL(18,2) AND e >= 5.000::DECIMAL(38,3)",
        ),
        (
            "decimal_transitive_unsafe_common_type",
            "A safe equality chain must not introduce direct comparisons whose common Decimal precision would overflow.",
            "SELECT a FROM infer_decimal_chain WHERE a = b AND b = c AND c >= 5.00::DECIMAL(38,2) AND a < 8::DECIMAL(37,0)",
        ),
        (
            "integer_out_of_range",
            "An out-of-range constant is retained rather than truncated to the column type.",
            "SELECT y FROM infer_bounds WHERE y > 0 AND y < 10 AND y < 2147483648",
        ),
        (
            "decimal_rounding_cast",
            "Constraints on differently scaled casts cannot be applied to the uncast column.",
            "SELECT d FROM infer_bounds WHERE CAST(d AS DECIMAL(18,1)) = 1.0::DECIMAL(18,1) AND d > 1.0::DECIMAL(18,2)",
        ),
    ];
    for (name, description, sql) in cases {
        let case = SqlTestCase {
            name,
            description,
            setup_sqls,
            sql,
        };
        let ctx = setup_context(&case).await?;
        let raw_plan = ctx.bind_sql(sql).await?;
        let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;
        let opt_ctx =
            OptimizerContext::new(ctx.clone(), Default::default(), ctx.get_function_context()?);
        let stat_ctx = opt_ctx.get_stat_context();
        write_case_header(file, &case)?;
        writeln!(file, "raw_plan:")?;
        writeln!(
            file,
            "{}",
            raw_plan.format_indent(Default::default(), stat_ctx)?
        )?;
        writeln!(file, "optimized_plan:")?;
        writeln!(
            file,
            "{}",
            optimized_plan.format_indent(Default::default(), stat_ctx)?
        )?;
        writeln!(file)?;
    }
    Ok(())
}
