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

use std::f64::consts::E;
use std::fmt;

use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::buffer::Buffer;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct GenericLogFunction {
    display_name: String,
    default_base: f64,
    unary: bool,
}

impl GenericLogFunction {
    pub fn try_create(
        display_name: &str,
        default_base: f64,
        unary: bool,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            default_base,
            unary,
        }))
    }
}

impl Function for GenericLogFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        if self.unary {
            1
        } else {
            0
        }
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        if self.unary {
            None
        } else {
            Some((1, 2))
        }
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
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
            let num_series = columns[1]
                .column()
                .to_minimal_array()?
                .cast_with_type(&DataType::Float64)?;
            match base_column {
                DataColumn::Constant(v, _) => {
                    let base = DFTryFrom::try_from(v.clone())?;
                    num_series.f64()?.apply_cast_numeric(|v| v.log(base))
                }
                DataColumn::Array(base_series) => {
                    let validity = combine_validities(
                        num_series.get_array_ref().validity(),
                        base_series.get_array_ref().validity(),
                    );
                    let values = Buffer::from_trusted_len_iter(
                        num_series
                            .f64()?
                            .into_no_null_iter()
                            .zip(base_series.f64()?.into_no_null_iter())
                            .map::<f64, _>(|(num, base)| num.log(*base)),
                    );
                    let array = PrimitiveArray::<f64>::from_data(
                        DataType::Float64.to_arrow(),
                        values,
                        validity,
                    );
                    DFFloat64Array::from_arrow_array(&array)
                }
            }
        };

        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for GenericLogFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LOG")
    }
}

pub struct LogFunction {}

impl LogFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        GenericLogFunction::try_create(display_name, E, false)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct LnFunction {}

impl LnFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        GenericLogFunction::try_create(display_name, E, true)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}
