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

use std::f64::consts::E;
use std::fmt;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct GenericLogFunction {
    display_name: String,
    default_base: f64,
}

impl GenericLogFunction {
    pub fn try_create(display_name: &str, default_base: f64) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            default_base,
        }))
    }
}

impl Function for GenericLogFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[DataTypeAndNullable]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let result = if columns.len() == 1 {
            // Log(num) with default_base if one arg
            let num_series = columns[0]
                .column()
                .to_minimal_array()?
                .cast_with_type(&DataType::Float64)?;
            num_series
                .f64()?
                .apply_cast_numeric(|v| v.log(self.default_base))
        } else {
            // Log(base, num) if two args
            let base_column: &DataColumn =
                &columns[0].column().cast_with_type(&DataType::Float64)?;
            let num_column: &DataColumn =
                &columns[1].column().cast_with_type(&DataType::Float64)?;

            match (base_column, num_column) {
                (DataColumn::Array(base_series), DataColumn::Constant(v, _)) => {
                    if v.is_null() {
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        let v: f64 = DFTryFrom::try_from(v.clone())?;
                        base_series.f64()?.apply_cast_numeric(|base| v.log(base))
                    }
                }
                (DataColumn::Constant(base, _), DataColumn::Array(num_series)) => {
                    if base.is_null() {
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        let base = DFTryFrom::try_from(base.clone())?;
                        num_series.f64()?.apply_cast_numeric(|v| v.log(base))
                    }
                }
                _ => {
                    let base_series = base_column.to_minimal_array()?;
                    let num_series = num_column.to_minimal_array()?;

                    // The log function has default null behavior for null input, that is, LOG(null) = null.
                    // So the passthrough_null method has default behavior to be true, we don't need to zip validity.
                    binary_with_validity(
                        num_series.f64()?,
                        base_series.f64()?,
                        |num, base| num.log(base),
                        None,
                    )
                }
            }
        };

        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for GenericLogFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

pub struct LogFunction {}

impl LogFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        GenericLogFunction::try_create(display_name, E)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }
}

pub struct LnFunction {}

impl LnFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        GenericLogFunction::try_create(display_name, E)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct Log10Function {}

impl Log10Function {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        GenericLogFunction::try_create(display_name, 10_f64)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct Log2Function {}

impl Log2Function {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        GenericLogFunction::try_create(display_name, 2_f64)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}
