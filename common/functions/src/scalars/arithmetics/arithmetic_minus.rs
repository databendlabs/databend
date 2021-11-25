// Copyright 2021 Datafuse Labs.
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

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

pub struct ArithmeticMinusFunction;

impl ArithmeticMinusFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Minus)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().monotonicity())
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        // unary operation like '-f(x)', just flip the is_positive.
        // also pass the is_constant, in case the input is a constant value.
        if args.len() == 1 {
            return Ok(Monotonicity {
                is_monotonic: args[0].is_monotonic || args[0].is_constant,
                is_positive: !args[0].is_positive,
                is_constant: args[0].is_constant,
                left: None,
                right: None,
            });
        }

        // For expression f(x) - g(x), only when both f(x) and g(x) are monotonic and have
        // opposite 'is_positive' can we get a monotonic expression.
        let f_x = &args[0];
        let g_x = &args[1];

        // case of 12 - g(x)
        if f_x.is_constant {
            return Ok(Monotonicity {
                is_monotonic: g_x.is_monotonic || g_x.is_constant,
                is_positive: !g_x.is_positive,
                is_constant: g_x.is_constant,
                left: None,
                right: None,
            });
        }

        // case of f(x) - 12
        if g_x.is_constant {
            return Ok(Monotonicity {
                is_monotonic: f_x.is_monotonic,
                is_positive: f_x.is_positive,
                is_constant: f_x.is_constant,
                left: None,
                right: None,
            });
        }

        // if either one is non-monotonic, return non-monotonic
        if !f_x.is_monotonic || !g_x.is_monotonic {
            return Ok(Monotonicity::default());
        }

        // when both are monotonic, and have same 'is_positive', we can't determine the monotonicity
        if f_x.is_positive == g_x.is_positive {
            return Ok(Monotonicity::default());
        }

        Ok(Monotonicity {
            is_monotonic: true,
            is_positive: f_x.is_positive,
            is_constant: false,
            left: None,
            right: None,
        })
    }
}
