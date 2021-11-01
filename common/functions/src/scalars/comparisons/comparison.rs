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

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::CastFunction;
use crate::scalars::ComparisonEqFunction;
use crate::scalars::ComparisonGtEqFunction;
use crate::scalars::ComparisonGtFunction;
use crate::scalars::ComparisonLikeFunction;
use crate::scalars::ComparisonLtEqFunction;
use crate::scalars::ComparisonLtFunction;
use crate::scalars::ComparisonNotEqFunction;
use crate::scalars::ComparisonNotLikeFunction;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ComparisonFunction {
    op: DataValueComparisonOperator,
}

impl ComparisonFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("=", ComparisonEqFunction::desc());
        factory.register("<", ComparisonLtFunction::desc());
        factory.register(">", ComparisonGtFunction::desc());
        factory.register("<=", ComparisonLtEqFunction::desc());
        factory.register(">=", ComparisonGtEqFunction::desc());
        factory.register("!=", ComparisonNotEqFunction::desc());
        factory.register("<>", ComparisonNotEqFunction::desc());
        factory.register("like", ComparisonLikeFunction::desc());
        factory.register("not like", ComparisonNotLikeFunction::desc());
    }

    pub fn try_create_func(op: DataValueComparisonOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonFunction { op }))
    }
}

impl Function for ComparisonFunction {
    fn name(&self) -> &str {
        "ComparisonFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        if columns[0].data_type() != columns[1].data_type() {
            let compare_coercion_type =
                compare_coercion(columns[0].data_type(), columns[1].data_type())?;

            let col0 = cast_column(&columns[0], compare_coercion_type.clone(), input_rows)?;
            let col1 = cast_column(&columns[1], compare_coercion_type, input_rows)?;
            return self.eval(&[col0, col1], input_rows);
        }

        columns[0]
            .column()
            .compare(self.op.clone(), columns[1].column())
    }

    fn num_arguments(&self) -> usize {
        2
    }
}

impl fmt::Display for ComparisonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}

fn cast_column(
    column: &DataColumnWithField,
    data_type: DataType,
    input_rows: usize,
) -> Result<DataColumnWithField> {
    let new_col = CastFunction::create("cast".to_string(), data_type.clone())
        .unwrap()
        .eval(&[column.clone()], input_rows)?;

    let new_field = DataField::new(column.field().name(), data_type, false);
    Ok(DataColumnWithField::new(new_col, new_field))
}
