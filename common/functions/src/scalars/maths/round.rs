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

use std::cmp::Ordering;
use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
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

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 2))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let x_series = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::Float64)?;

        let column: DataColumn = if columns.len() == 1 {
            x_series.f64()?.apply(|x| (self.rounding_func)(x))
        } else {
            let d_column: &DataColumn = &columns[1].column().cast_with_type(&DataType::Int64)?;
            match d_column {
                DataColumn::Constant(v, _) => {
                    if v.is_null() {
                        // mysql> SELECT ROUND(1.015, NULL); => NULL
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        match v.as_i64() {
                            Err(_) => {
                                // mysql> SELECT ROUND(1.015, 'aaa'); => 1 => d == 0
                                x_series.f64()?.apply(|x| (self.rounding_func)(x))
                            }
                            Ok(v) => match v.cmp(&0) {
                                Ordering::Greater => {
                                    let z = 10_f64.powi(if v > 30 { 30 } else { v as i32 });
                                    x_series.f64()?.apply(|x| {
                                        x.trunc() + (self.rounding_func)(x.fract() * z) / z
                                    })
                                }
                                Ordering::Less => {
                                    let z = 10_f64.powi(if v < -30 { 30 } else { -v as i32 });
                                    x_series.f64()?.apply(|x| (self.rounding_func)(x / z) * z)
                                }
                                Ordering::Equal => {
                                    x_series.f64()?.apply(|x| (self.rounding_func)(x))
                                }
                            },
                        }
                    }
                }
                DataColumn::Array(d_series) => {
                    let round = |x: &f64, d: &i64| {
                        let v = *d;
                        match v.cmp(&0) {
                            Ordering::Greater => Some({
                                let z = 10_f64.powi(if v > 30 { 30 } else { v as i32 });
                                x.trunc() + (self.rounding_func)(x.fract() * z) / z
                            }),
                            Ordering::Less => Some({
                                let z = 10_f64.powi(if v < -30 { 30 } else { -v as i32 });
                                (self.rounding_func)(x / z) * z
                            }),
                            Ordering::Equal => Some((self.rounding_func)(*x)),
                        }
                    };

                    let opt_iter = x_series
                        .f64()?
                        .into_iter()
                        .zip(d_series.i64()?.into_iter())
                        .map(|(x, d)| match (x, d) {
                            (Some(x), Some(d)) => round(x, d),
                            _ => None,
                        });

                    DFFloat64Array::new_from_opt_iter(opt_iter)
                }
            }
        }
        .into();
        Ok(column.resize_constant(columns[0].column().len()))
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
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TruncNumberFunction {}

impl TruncNumberFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        RoundingFunction::try_create(display_name, f64::trunc)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}
