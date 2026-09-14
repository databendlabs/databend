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

use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::ScalarRef;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

fn build_nested_abs(depth: usize) -> RawExpr {
    let leaf = RawExpr::ColumnRef {
        span: None,
        id: 0usize,
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "x".to_string(),
    };

    let mut expr = leaf;
    for _ in 0..depth {
        expr = RawExpr::FunctionCall {
            span: None,
            name: "abs".to_string(),
            params: vec![],
            args: vec![expr],
        };
    }
    expr
}

fn build_add_chain(depth: usize) -> RawExpr {
    let col = RawExpr::ColumnRef {
        span: None,
        id: 0usize,
        data_type: DataType::Number(NumberDataType::Int32),
        display_name: "x".to_string(),
    };

    let mut expr = col.clone();
    for _ in 1..depth {
        expr = RawExpr::FunctionCall {
            span: None,
            name: "plus".to_string(),
            params: vec![],
            args: vec![expr, col.clone()],
        };
    }
    expr
}

#[test]
fn test_deep_nested_abs_no_stack_overflow() {
    let raw_expr = build_nested_abs(1000);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();

    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32])]);
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let result = evaluator.run(&expr).unwrap();

    let scalar = result.index(0).unwrap();
    assert_eq!(scalar, ScalarRef::Number(NumberScalar::UInt64(1)));
}

#[test]
fn test_deep_add_chain_no_stack_overflow() {
    let raw_expr = build_add_chain(2000);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();

    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32])]);
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let result = evaluator.run(&expr).unwrap();

    let scalar = result.index(0).unwrap();
    assert_eq!(scalar, ScalarRef::Number(NumberScalar::Int64(2000)));
}
