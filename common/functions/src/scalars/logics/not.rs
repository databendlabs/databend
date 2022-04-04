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
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionFeatures;
use crate::scalars::TypedFunctionDescription;

#[derive(Clone)]
pub struct LogicNotExpression;

impl LogicExpression for LogicNotExpression {
    fn eval(columns: &ColumnsWithField, input_rows: usize, _nullable: bool) -> Result<ColumnRef> {
        let col = cast_column_field(&columns[0], &BooleanType::arc())?;

        let col_viewer = bool::try_create_viewer(&col)?;

        let mut builder = ColumnBuilder::<bool>::with_capacity(input_rows);

        for value in col_viewer.iter() {
            builder.append(!value);
        }
        Ok(builder.build(input_rows))
    }
}

#[derive(Clone)]
pub struct LogicNotFunction;

impl LogicNotFunction {
    pub fn try_create(_display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        LogicFunctionImpl::<LogicNotExpression>::try_create(LogicOperator::Not, args)
    }

    pub fn desc() -> TypedFunctionDescription {
        TypedFunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}
