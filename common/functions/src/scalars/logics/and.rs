// Copyright 2022 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;

use super::logic::LogicExpression;
use super::logic::LogicFunctionImpl;
use super::logic::LogicOperator;
use crate::calcute;
use crate::impl_logic_expression;
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

impl_logic_expression!(LogicAndExpression, &, |lhs: bool, rhs: bool, lhs_v: bool, rhs_v: bool| -> (bool, bool) {
    (lhs & rhs,  (lhs_v & rhs_v) | (!lhs & lhs_v) | (!rhs & rhs_v))
});

#[derive(Clone)]
pub struct LogicAndFunction;

impl LogicAndFunction {
    pub fn try_create(_display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        LogicFunctionImpl::<LogicAndExpression>::try_create(LogicOperator::And, args)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null()
                .num_arguments(2),
        )
    }
}
