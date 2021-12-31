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

use std::cmp::Ordering;
use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
struct RoundingFunction {
    display_name: String,
    rounding_func: fn(f64) -> f64,
}

impl RoundingFunction {
    pub fn try_create(display_name: &str, f: fn(f64) -> f64) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            rounding_func: f,
        }))
    }
}

impl Function for RoundingFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[DataTypeAndNullable]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let round = |x: f64, d: i64| match d.cmp(&0) {
            Ordering::Greater => {
                let z = 10_f64.powi(if d > 30 { 30 } else { d as i32 });
                (self.rounding_func)(x * z) / z
            }
            Ordering::Less => {
                let z = 10_f64.powi(if d < -30 { 30 } else { -d as i32 });
                (self.rounding_func)(x / z) * z
            }
            Ordering::Equal => (self.rounding_func)(x),
        };

        let x_column: &DataColumn = &columns[0].column().cast_with_type(&DataType::Float64)?;

        let r_column: DataColumn = if columns.len() == 1 {
            x_column
                .to_minimal_array()?
                .f64()?
                .apply(|x| (self.rounding_func)(x))
        } else {
            let d_column: &DataColumn = &columns[1].column().cast_with_type(&DataType::Int64)?;

            match (x_column, d_column) {
                (DataColumn::Array(x_series), DataColumn::Constant(d, _)) => {
                    if d.is_null() {
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        let x_arr = x_series.f64()?;
                        let v: i64 = DFTryFrom::try_from(d.clone())?;
                        match v.cmp(&0) {
                            Ordering::Greater => {
                                let z = 10_f64.powi(if v > 30 { 30 } else { v as i32 });
                                x_arr.apply(|x| (self.rounding_func)(x * z) / z)
                            }
                            Ordering::Less => {
                                let z = 10_f64.powi(if v < -30 { 30 } else { -v as i32 });
                                x_arr.apply(|x| (self.rounding_func)(x / z) * z)
                            }
                            Ordering::Equal => x_arr.apply(|x| (self.rounding_func)(x)),
                        }
                    }
                }
                (DataColumn::Constant(x, _), DataColumn::Array(d_series)) => {
                    if x.is_null() {
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        let x: f64 = DFTryFrom::try_from(x.clone())?;
                        d_series.i64()?.apply_cast_numeric(|d| round(x, d))
                    }
                }
                _ => {
                    let x_series = x_column.to_minimal_array()?;
                    let d_series = d_column.to_minimal_array()?;
                    binary(x_series.f64()?, d_series.i64()?, round)
                }
            }
        }
        .into();
        Ok(r_column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for RoundingFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub struct RoundNumberFunction {}

impl RoundNumberFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        RoundingFunction::try_create(display_name, f64::round)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }
}

pub struct TruncNumberFunction {}

impl TruncNumberFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        RoundingFunction::try_create(display_name, f64::trunc)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }
}
