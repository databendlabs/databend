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
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::scalars::MonotonicityNode;
use crate::scalars::Range;

pub struct ArithmeticModuloFunction;

impl ArithmeticModuloFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Modulo)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }

    pub fn get_monotonicity(_args: &[MonotonicityNode]) -> Result<MonotonicityNode> {
        //TODO: implement
        Ok(MonotonicityNode::Function(Monotonicity::default(), Range {
            begin: None,
            end: None,
        }))
    }
}
