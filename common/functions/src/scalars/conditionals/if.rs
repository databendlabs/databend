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
use common_datavalues::{DataField, DataTypeAndNullable};
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct IfFunction {
    _display_name: String,
}

impl IfFunction {
    pub fn try_create_func(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(IfFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl Function for IfFunction {
    fn name(&self) -> &str {
        "IfFunction"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        let mut aggregate_args = Vec::with_capacity(args.len() - 1);
        for index in 1..args.len() {
            aggregate_args.push(args[index].data_type().clone())
        }
        common_datavalues::aggregate_types(&aggregate_args)
    }

    // IF(condition, value_if_true, value_if_false) is nullable if 'value_if_true' is nullable or 'value_if_false' is nullable.
    // The condition of 'Null' is treated as false.
    fn nullable(&self, args: &[DataTypeAndNullable]) -> Result<bool> {
        let nullable = args[1].is_nullable() || args[2].is_nullable();
        Ok(nullable)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        columns[0]
            .column()
            .if_then_else(columns[1].column(), columns[2].column())
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl std::fmt::Display for IfFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IF")
    }
}
