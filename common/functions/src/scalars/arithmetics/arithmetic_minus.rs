// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;

use super::ArithmeticPlusFunction;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::scalars::MonotonicityNode;
use crate::scalars::Range;

pub struct ArithmeticMinusFunction;

impl ArithmeticMinusFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Minus)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }

    // get monotonicity from unary negate, like -x should flip the monotonicity and range
    fn get_unary_operator_monotonicity(node: &MonotonicityNode) -> Result<MonotonicityNode> {
        let func = ArithmeticFunction::new(DataValueArithmeticOperator::Minus);

        match node {
            MonotonicityNode::Function(mono, Range { begin, end }) => {
                if !mono.is_monotonic {
                    return Ok(node.clone());
                }
                let new_begin = func.eval_range_boundary(&[begin.clone()])?;
                let new_end = func.eval_range_boundary(&[end.clone()])?;
                Ok(MonotonicityNode::Function(mono.flip_clone(), Range {
                    begin: new_end,
                    end: new_begin,
                }))
            }
            MonotonicityNode::Constant(data_column_field) => {
                let new_val = func.eval_range_boundary(&[Some(data_column_field.clone())])?;
                Ok(MonotonicityNode::Constant(new_val.unwrap()))
            }
            MonotonicityNode::Variable(_column_name, Range { begin, end }) => {
                let new_begin = func.eval_range_boundary(&[begin.clone()])?;
                let new_end = func.eval_range_boundary(&[end.clone()])?;
                Ok(MonotonicityNode::Function(
                    Monotonicity {
                        is_monotonic: true,
                        is_positive: false,
                    },
                    Range {
                        begin: new_end,
                        end: new_begin,
                    },
                ))
            }
        }
    }

    pub fn get_monotonicity(args: &[MonotonicityNode]) -> Result<MonotonicityNode> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        if args.len() == 1 {
            return Self::get_unary_operator_monotonicity(&args[0]);
        }

        ArithmeticPlusFunction::get_plus_minus_binary_operation_monotonicity(
            DataValueArithmeticOperator::Minus,
            args[0].clone(),
            args[1].clone(),
        )
    }
}
