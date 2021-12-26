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

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataType;
use common_datavalues::DataValueLogicOperator;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::Function;
use crate::scalars::LogicAndFunction;
use crate::scalars::LogicNotFunction;
use crate::scalars::LogicOrFunction;

#[derive(Clone)]
pub struct LogicFunction {
    op: DataValueLogicOperator,
}

impl LogicFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("and", LogicAndFunction::desc());
        factory.register("or", LogicOrFunction::desc());
        factory.register("not", LogicNotFunction::desc());
    }

    pub fn try_create_func(op: DataValueLogicOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(LogicFunction { op }))
    }
}

impl Function for LogicFunction {
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let columns: Vec<DataColumn> = columns.iter().map(|c| c.column().clone()).collect();
        columns[0].logic(self.op.clone(), &columns[1..])
    }
}

impl fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
