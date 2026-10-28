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

use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::FromData;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::nullable::NullableDomain;

fn error_to_null_function(
    return_type: DataType,
    domain: FunctionDomain<AnyType>,
    output: Value<AnyType>,
    error_row: Option<usize>,
) -> Function {
    Function {
        signature: FunctionSignature {
            name: "test_error_to_null".to_string(),
            args_type: vec![],
            return_type,
        },
        eval: FunctionEval::Scalar {
            calc_domain: domain_evaluator(move |_, _| domain.clone()),
            eval: scalar_evaluator(move |_, ctx| {
                if let Some(row) = error_row {
                    ctx.set_error(row, "conversion failed");
                }
                output.clone()
            }),
            derive_stat: None,
        },
    }
    .error_to_null()
}

fn evaluate(function: Function, num_rows: usize) -> (FunctionDomain<AnyType>, Value<AnyType>) {
    let func_ctx = FunctionContext::default();
    let mut ctx = EvalContext {
        generics: &[],
        num_rows,
        func_ctx: &func_ctx,
        validity: None,
        errors: None,
        suppress_error: false,
        strict_eval: false,
    };
    let (calc_domain, _, eval) = function.eval.into_scalar().unwrap();
    let domain = calc_domain.domain_eval(&func_ctx, &[]);
    if let FunctionDomain::Domain(domain) = &domain {
        domain
            .check_data_type(&function.signature.return_type)
            .unwrap();
    }
    let output = eval.eval(&[], &mut ctx);
    if let Value::Column(column) = &output {
        assert_eq!(column.data_type(), function.signature.return_type);
    }
    assert!(ctx.errors.is_none());
    (domain, output)
}

#[test]
fn test_error_to_null_column() {
    for nullable in [false, true] {
        for has_error in [false, true] {
            let column = if nullable {
                Int32Type::from_opt_data(vec![Some(10), None, Some(30)])
            } else {
                Int32Type::from_data(vec![10, 20, 30])
            };
            let return_type = column.data_type();
            let inner_domain = column.domain();
            let function = error_to_null_function(
                return_type.clone(),
                if has_error {
                    FunctionDomain::MayThrow
                } else {
                    FunctionDomain::Domain(inner_domain.clone())
                },
                Value::Column(column),
                has_error.then_some(2),
            );
            assert_eq!(function.signature.return_type, return_type.wrap_nullable());
            let (domain, output) = evaluate(function, 3);
            let expected = Int32Type::from_opt_data(vec![
                Some(10),
                if nullable { None } else { Some(20) },
                if has_error { None } else { Some(30) },
            ]);
            assert_eq!(output, Value::Column(expected));
            assert_eq!(
                domain,
                if has_error {
                    FunctionDomain::Full
                } else if nullable {
                    FunctionDomain::Domain(inner_domain)
                } else {
                    FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                        has_null: false,
                        value: Some(Box::new(inner_domain)),
                    }))
                }
            );
        }
    }
}

#[test]
fn test_error_to_null_scalar() {
    for scalar in [Scalar::from(42_i32), Scalar::Null] {
        for has_error in [false, true] {
            let return_type = DataType::Number(NumberDataType::Int32).wrap_nullable();
            let function = error_to_null_function(
                return_type,
                FunctionDomain::Full,
                Value::Scalar(scalar.clone()),
                has_error.then_some(0),
            );
            let (domain, output) = evaluate(function, 1);
            assert_eq!(domain, FunctionDomain::Full);
            assert_eq!(
                output,
                Value::Scalar(if has_error {
                    Scalar::Null
                } else {
                    scalar.clone()
                })
            );
        }
    }
}

#[test]
fn test_error_to_null_all_null() {
    let column = Int32Type::from_opt_data(vec![None, None]);
    let domain = column.domain();
    let function = error_to_null_function(
        column.data_type(),
        FunctionDomain::Domain(domain.clone()),
        Value::Column(column.clone()),
        None,
    );
    let (output_domain, output) = evaluate(function, 2);
    assert_eq!(output_domain, FunctionDomain::Domain(domain));
    assert_eq!(output, Value::Column(column));
}
