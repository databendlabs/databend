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

use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::Pow;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct PowFunction {
    display_name: String,
}

impl PowFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(PowFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for PowFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0].is_numeric() || args[0] == DataType::String || args[0] == DataType::Null {
            Ok(DataType::Float64)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric, but got {}",
                args[0]
            )))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let x_column: &DataColumn = &columns[0].column().cast_with_type(&DataType::Float64)?;
        let y_column: &DataColumn = &columns[1].column().cast_with_type(&DataType::Float64)?;

        let result = match (x_column, y_column) {
            (DataColumn::Array(x_series), DataColumn::Constant(y, _)) => {
                if y.is_null() {
                    DFFloat64Array::full_null(input_rows)
                } else {
                    let y: f64 = DFTryFrom::try_from(y.clone())?;
                    x_series.f64()?.apply_cast_numeric(|x| x.pow(y))
                }
            }
            (DataColumn::Constant(x, _), DataColumn::Array(y_series)) => {
                if x.is_null() {
                    DFFloat64Array::full_null(input_rows)
                } else {
                    let x: f64 = DFTryFrom::try_from(x.clone())?;
                    y_series.f64()?.apply_cast_numeric(|y| x.pow(y))
                }
            }
            _ => {
                let x_series = x_column.to_minimal_array()?;
                let y_series = y_column.to_minimal_array()?;
                binary(x_series.f64()?, y_series.f64()?, |x, y| x.pow(y))
            }
        };
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for PowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
