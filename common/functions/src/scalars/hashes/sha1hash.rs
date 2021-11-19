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
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct Sha1HashFunction {
    display_name: String,
}

impl Sha1HashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Sha1HashFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for Sha1HashFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0] == DataType::String {
            Ok(DataType::String)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected string type, but got {}",
                args[0]
            )))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let opt_iter = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::String)?;

        let opt_iter = opt_iter
            .string()?
            .into_iter()
            .map(|vo| vo.map(|v| sha1::Sha1::from(v).digest().to_string()));

        let result = DFStringArray::new_from_opt_iter(opt_iter);
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for Sha1HashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
