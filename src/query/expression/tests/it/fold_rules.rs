// Copyright 2026 Datafuse Labs.
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

//! Tests of `FoldRules`. The rules take constants, types and domains, so
//! these tests build expressions only to resolve builtin functions or, for
//! `eval_constant`, to evaluate them.

use databend_common_expression::Domain;
use databend_common_expression::FilterDomain;
use databend_common_expression::FoldRules;
use databend_common_expression::FunctionContext;
use databend_common_expression::IfPruning;
use databend_common_expression::RangeConstraint;
use databend_common_expression::Scalar;
use databend_common_expression::expr::ColumnRef;
use databend_common_expression::expr::Expr;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;

fn builtin_call(name: &str, args: &[Expr<usize>]) -> Expr<usize> {
    type_check::check_function(None, name, &[], args, &BUILTIN_FUNCTIONS).unwrap()
}

fn uint8_domain(min: u8, max: u8) -> Domain {
    Domain::Number(NumberDomain::UInt8(SimpleDomain { min, max }))
}

#[test]
fn test_if_pruning_rule() {
    let t = Some(Scalar::Boolean(true));
    let f = Some(Scalar::Boolean(false));
    let null = Some(Scalar::Null);
    let cases: Vec<(Vec<Option<Scalar>>, IfPruning)> = vec![
        (vec![t.clone(), None, None], IfPruning::Select(1)),
        (vec![f.clone(), None, None], IfPruning::Select(2)),
        (vec![null, None, None], IfPruning::Select(2)),
        (vec![None, None, None], IfPruning::Unchanged),
        (vec![None, None, None, None, None], IfPruning::Unchanged),
        (
            vec![None, None, t, None, None],
            IfPruning::Keep(vec![0, 1, 3]),
        ),
        (
            vec![None, None, f.clone(), None, None],
            IfPruning::Keep(vec![0, 1, 4]),
        ),
        (
            vec![f, None, None, None, None],
            IfPruning::Keep(vec![2, 3, 4]),
        ),
    ];
    for (args, expected) in cases {
        let pruning = FoldRules::if_pruning(args.len(), |index| args[index].as_ref());
        assert_eq!(pruning, expected, "{args:?}");
    }
}

#[test]
fn test_filter_domain_rule() {
    let null = Domain::Nullable(NullableDomain {
        has_null: true,
        value: None,
    });
    let only_true = Domain::Boolean(BooleanDomain {
        has_false: false,
        has_true: true,
    });

    let mut and = FilterDomain::new(false);
    assert!(and.is_absorbing(&Scalar::Boolean(false)));
    assert!(!and.is_absorbing(&Scalar::Boolean(true)));
    and.add(Some(only_true.clone()));
    assert_eq!(and.singleton(), Some(true));
    // Filters treat NULL as false, which absorbs AND.
    and.add(Some(null.clone()));
    assert_eq!(and.singleton(), Some(and.absorbing_value()));

    let mut or = FilterDomain::new(true);
    or.add(Some(null));
    assert_eq!(or.singleton(), Some(false));
    or.add(Some(only_true));
    assert_eq!(or.singleton(), Some(or.absorbing_value()));

    let mut unknown = FilterDomain::new(false);
    unknown.add(None);
    assert_eq!(unknown.domain(), None);
    assert_eq!(unknown.singleton(), None);
}

#[test]
fn test_function_domain_rule() {
    let uint8 = DataType::Number(NumberDataType::UInt8);
    let column = |id| {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id,
            data_type: uint8.clone(),
            display_name: format!("c{id}"),
        })
    };
    let Expr::FunctionCall(call) = builtin_call("plus", &[column(0), column(1)]) else {
        panic!("plus should resolve to a function call");
    };
    let domain = FoldRules::function_domain(
        &FunctionContext::default(),
        &BUILTIN_FUNCTIONS,
        &call.function,
        &call.generics,
        &call.return_type,
        Some(&uint8),
        &[uint8_domain(1, 2), uint8_domain(3, 3)],
    );
    assert_eq!(
        domain,
        Some(Domain::Number(NumberDomain::UInt16(SimpleDomain {
            min: 4,
            max: 5
        })))
    );

    let string = DataType::String;
    let Expr::FunctionCall(call) = builtin_call("to_int64", &[Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: string.clone(),
        display_name: "s".to_string(),
    })]) else {
        panic!("to_int64 should resolve to a function call");
    };
    let domain = FoldRules::function_domain(
        &FunctionContext::default(),
        &BUILTIN_FUNCTIONS,
        &call.function,
        &call.generics,
        &call.return_type,
        Some(&string),
        &[Domain::full(&string)],
    );
    assert_eq!(domain, None, "a call that may throw has no domain");
}

#[test]
fn test_cast_domain_rule() {
    let uint8 = DataType::Number(NumberDataType::UInt8);
    let int64 = DataType::Number(NumberDataType::Int64);
    let cast_domain = |src: &DataType, dest: &DataType, is_try, domain: &Domain| {
        FoldRules::cast_domain(
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            None,
            src,
            dest,
            is_try,
            domain,
        )
    };

    assert_eq!(
        cast_domain(&uint8, &int64, false, &uint8_domain(1, 2)),
        Some(Domain::Number(NumberDomain::Int64(SimpleDomain {
            min: 1,
            max: 2
        })))
    );
    assert_eq!(
        cast_domain(&int64, &uint8, false, &Domain::full(&int64)),
        None,
        "a cast that may overflow has no domain"
    );
    assert!(
        matches!(
            cast_domain(&int64, &uint8.wrap_nullable(), true, &Domain::full(&int64)),
            Some(Domain::Nullable(NullableDomain { has_null: true, .. }))
        ),
        "TRY_CAST turns overflow into NULL"
    );
}

#[test]
fn test_eval_constant_rule() {
    let columns = [("a", DataType::Number(NumberDataType::UInt8))];
    let eval = |text: &str| {
        let raw_expr = parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS);
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        FoldRules::eval_constant(&FunctionContext::default(), &BUILTIN_FUNCTIONS, &expr)
    };

    assert_eq!(eval("1 + 2"), Some(Scalar::Number(NumberScalar::UInt16(3))));
    assert_eq!(eval("a + 1"), None, "column references are not evaluated");
    assert_eq!(
        eval("to_uint8(300)"),
        None,
        "a failing expression is left for runtime"
    );
}

#[test]
fn test_ranges_mutually_exclusive_rule() {
    let uint64 = DataType::Number(NumberDataType::UInt64);
    let constraint =
        |column_id: usize, data_type: &DataType, operator: &str, value: u64| RangeConstraint {
            column_id,
            data_type: data_type.clone(),
            operator: operator.to_string(),
            constant: Scalar::Number(NumberScalar::UInt64(value)),
            is_flipped: false,
        };
    let exclusive = |constraints: Vec<RangeConstraint<usize>>| {
        FoldRules::ranges_mutually_exclusive(constraints)
    };

    assert!(exclusive(vec![
        constraint(0, &uint64, "gt", 5),
        constraint(0, &uint64, "lt", 3),
    ]));
    assert!(!exclusive(vec![
        constraint(0, &uint64, "gt", 5),
        constraint(0, &uint64, "lt", 10),
    ]));
    assert!(
        !exclusive(vec![
            constraint(0, &uint64, "gt", 5),
            constraint(1, &uint64, "lt", 3),
        ]),
        "constraints on different columns do not conflict"
    );
    assert!(
        !exclusive(vec![
            constraint(0, &uint64, "gt", 5),
            constraint(0, &uint64.wrap_nullable(), "lt", 3),
        ]),
        "the same column under different types is kept separate"
    );
    assert!(
        !exclusive(vec![
            constraint(0, &DataType::Number(NumberDataType::Int64), "gt", 5),
            constraint(0, &DataType::Number(NumberDataType::Int64), "lt", 3),
        ]),
        "a constant of another type is not compared"
    );
}
