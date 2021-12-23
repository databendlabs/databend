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

use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ComparisonFunction;
use crate::scalars::Function;

pub struct ComparisonGtFunction;

impl ComparisonGtFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::Gt)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("<=")
                .bool_function()
                .num_arguments(2),
        )
    }
}
