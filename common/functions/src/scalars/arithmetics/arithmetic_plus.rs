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

use crate::scalars::function::MonotonicityNode;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

pub struct ArithmeticPlusFunction;

impl ArithmeticPlusFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Plus)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }

    pub fn get_monotonicity(args: &[MonotonicityNode]) -> Result<MonotonicityNode> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        if args.len() == 1 {
            return Ok(args[0].clone());
        }

        match (&args[0], &args[1]) {
            // a constant value plus a function, like 12 + f(x), the monotonicity should be the same as f
            (MonotonicityNode::Function(mono), MonotonicityNode::Constant(_))
            | (MonotonicityNode::Constant(_), MonotonicityNode::Function(mono)) => {
                Ok(MonotonicityNode::Function(mono.clone()))
            }

            // a constant value plus a variable, like x + 12 should be monotonically increasing
            (MonotonicityNode::Constant(_), MonotonicityNode::Variable(_))
            | (MonotonicityNode::Variable(_), MonotonicityNode::Constant(_)) => {
                Ok(MonotonicityNode::Function(Monotonicity {
                    is_monotonic: true,
                    is_positive: true,
                    is_always_monotonic: true,
                }))
            }

            // two function plus, f(x) + g(x) , if they have same monotonicity, then should return that monotonicity
            (MonotonicityNode::Function(mono1), MonotonicityNode::Function(mono2)) => {
                if mono1.is_monotonic
                    && mono2.is_monotonic
                    && mono1.is_positive == mono2.is_positive
                {
                    return Ok(MonotonicityNode::Function(Monotonicity {
                        is_monotonic: true,
                        is_positive: mono1.is_positive,
                        is_always_monotonic: mono1.is_always_monotonic && mono2.is_always_monotonic,
                    }));
                }
                Ok(MonotonicityNode::Function(Monotonicity::default()))
            }

            // two variable plus(we assume only one variable exists), like x+x, should be monotonically increasing
            (MonotonicityNode::Variable(var1), MonotonicityNode::Variable(var2)) => {
                if var1 == var2 {
                    return Ok(MonotonicityNode::Function(Monotonicity {
                        is_monotonic: true,
                        is_positive: true,
                        is_always_monotonic: true,
                    }));
                }
                Ok(MonotonicityNode::Function(Monotonicity::default()))
            }

            // a function plus the variable, like f(x) + x, if f(x) is monotonically increasing, return it.
            (MonotonicityNode::Function(mono), MonotonicityNode::Variable(_))
            | (MonotonicityNode::Variable(_), MonotonicityNode::Function(mono)) => {
                if mono.is_monotonic && mono.is_positive {
                    return Ok(MonotonicityNode::Function(mono.clone()));
                }
                Ok(MonotonicityNode::Function(Monotonicity::default()))
            }

            _ => Ok(MonotonicityNode::Function(Monotonicity::default())),
        }
    }
}
