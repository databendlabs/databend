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
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValueLogicOperator;
use common_exception::Result;

use crate::scalars::FactoryFuncRef;
use crate::scalars::Function;
use crate::scalars::LogicAndFunction;
use crate::scalars::LogicNotFunction;
use crate::scalars::LogicOrFunction;

#[derive(Clone)]
pub struct LogicFunction {
    op: DataValueLogicOperator,
}

impl LogicFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("and".into(), LogicAndFunction::try_create_func);
        map.insert("or".into(), LogicOrFunction::try_create_func);
        map.insert("not".into(), LogicNotFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(op: DataValueLogicOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(LogicFunction { op }))
    }
}

impl Function for LogicFunction {
    fn name(&self) -> &str {
        "LogicFunction"
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 2))
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        columns[0].logic(self.op.clone(), &columns[1..])
    }
}

impl fmt::Display for LogicFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
