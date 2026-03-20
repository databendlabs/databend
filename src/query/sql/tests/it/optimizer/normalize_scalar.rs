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
use databend_common_expression::Symbol;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Visibility;
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
use databend_common_sql_test_support::parse_raw_expr;

use crate::framework::golden::open_golden_file;
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
                Symbol::from_field_index(index),
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

fn write_case(
    file: &mut impl Write,
    case: &Case,
    columns: &[(&str, DataType)],
) -> Result<()> {
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

#[test]
fn test_normalize_scalar_filter_rule_outcomes() -> Result<()> {
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
            name: "scan_short_circuits_pushdown_or_chain",
            description: "Scan pushdown predicates should also short-circuit truthy OR operands.",
            target: Target::Scan,
            expr_text: "a = 3 or true or a = 5",
        },
    ];

    for case in &cases {
        write_case(&mut file, case, columns)?;
    }

    Ok(())
}
