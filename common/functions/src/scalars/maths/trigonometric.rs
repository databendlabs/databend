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
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct TrigonometricFunction {
    t: Trigonometric,
}

#[derive(Clone, Debug)]
pub enum Trigonometric {
    SIN,
    COS,
    COT,
    TAN,
    ACOS,
    ASIN,
    ATAN,
    ATAN2,
}

impl fmt::Display for Trigonometric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Trigonometric::SIN => "sin",
            Trigonometric::COS => "cos",
            Trigonometric::TAN => "tan",
            Trigonometric::COT => "cot",
            Trigonometric::ACOS => "acos",
            Trigonometric::ASIN => "asin",
            Trigonometric::ATAN => "atan",
            Trigonometric::ATAN2 => "atan2",
        };
        write!(f, "{}", display)
    }
}

impl TrigonometricFunction {
    pub fn try_create_func(t: Trigonometric) -> Result<Box<dyn Function>> {
        Ok(Box::new(TrigonometricFunction { t }))
    }
}

impl Function for TrigonometricFunction {
    fn name(&self) -> &str {
        "TrigonometricFunction"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if args[0].is_numeric() || args[0].is_string() || args[0].is_null() {
            Ok(DataType::Float64)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric, but got {}",
                args[0]
            )))
        }
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let result = if columns.len() == 1 {
            let opt_iter = columns[0]
                .column()
                .to_minimal_array()?
                .cast_with_type(&DataType::Float64)?;
            let opt_iter = opt_iter.f64()?.into_iter().map(|v| {
                v.and_then(|&v| match self.t {
                    Trigonometric::COS => Some(v.cos()),
                    Trigonometric::SIN => Some(v.sin()),
                    Trigonometric::COT => Some(1.0 / v.tan()),
                    Trigonometric::TAN => Some(v.tan()),
                    Trigonometric::ACOS => {
                        if (-1_f64..=1_f64).contains(&v) {
                            Some(v.acos())
                        } else {
                            None
                        }
                    }
                    Trigonometric::ASIN => {
                        if (-1_f64..=1_f64).contains(&v) {
                            Some(v.asin())
                        } else {
                            None
                        }
                    }
                    Trigonometric::ATAN => Some(v.atan()),
                    _ => unreachable!(),
                })
            });
            DFFloat64Array::new_from_opt_iter(opt_iter)
        } else {
            let y_column: &DataColumn = &columns[0].column().cast_with_type(&DataType::Float64)?;
            let x_column: &DataColumn = &columns[1].column().cast_with_type(&DataType::Float64)?;

            match (y_column, x_column) {
                (DataColumn::Array(y_series), DataColumn::Constant(x, _)) => {
                    if x.is_null() {
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        let x = DFTryFrom::try_from(x.clone())?;
                        y_series.f64()?.apply_cast_numeric(|y| y.atan2(x))
                    }
                }
                (DataColumn::Constant(y, _), DataColumn::Array(x_series)) => {
                    if y.is_null() {
                        DFFloat64Array::full_null(input_rows)
                    } else {
                        let y: f64 = DFTryFrom::try_from(y.clone())?;
                        x_series.f64()?.apply_cast_numeric(|x| y.atan2(x))
                    }
                }
                _ => {
                    let y_series = y_column.to_minimal_array()?;
                    let x_series = x_column.to_minimal_array()?;
                    binary(y_series.f64()?, x_series.f64()?, |y, x| y.atan2(x))
                }
            }
        };
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for TrigonometricFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.t)
    }
}

pub struct TrigonometricSinFunction;

impl TrigonometricSinFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::SIN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricCosFunction;

impl TrigonometricCosFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::COS)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricTanFunction;

impl TrigonometricTanFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::TAN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricCotFunction;

impl TrigonometricCotFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::COT)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricAsinFunction;

impl TrigonometricAsinFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ASIN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricAcosFunction;

impl TrigonometricAcosFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ACOS)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricAtanFunction;

impl TrigonometricAtanFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ATAN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }
}

pub struct TrigonometricAtan2Function;

impl TrigonometricAtan2Function {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ATAN2)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}
