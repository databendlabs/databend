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

use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
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

    fn num_arguments(&self) -> usize {
        match self.t {
            Trigonometric::ATAN2 => 2,
            _ => 1,
        }
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        match self.t {
            Trigonometric::ATAN => Some((1, 2)),
            _ => None,
        }
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if is_numeric(&args[0]) || args[0] == DataType::String || args[0] == DataType::Null {
            Ok(DataType::Float64)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric, but got {}",
                args[0]
            )))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result = if columns.len() == 1 {
            let opt_iter = columns[0]
                .column()
                .to_minimal_array()?
                .cast_with_type(&DataType::Float64)?;
            let opt_iter = opt_iter.f64()?.into_iter().map(|v| match v {
                Some(&v) => match self.t {
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
                },
                None => None,
            });
            DFFloat64Array::new_from_opt_iter(opt_iter)
        } else {
            let y_column: &DataColumn = &columns[0].column().cast_with_type(&DataType::Float64)?;
            let x_column: &DataColumn = &columns[1].column().cast_with_type(&DataType::Float64)?;

            match (y_column, x_column) {
                (DataColumn::Array(y_series), DataColumn::Constant(x, _)) => {
                    let x = DFTryFrom::try_from(x.clone())?;
                    let opt_iter = y_series
                        .f64()?
                        .into_no_null_iter()
                        .map(|y| Some(y.atan2(x)));
                    DFFloat64Array::new_from_opt_iter(opt_iter)
                }
                (DataColumn::Constant(y, _), DataColumn::Array(x_series)) => {
                    let y: f64 = DFTryFrom::try_from(y.clone())?;
                    let opt_iter = x_series
                        .f64()?
                        .into_no_null_iter()
                        .map(|x| Some(y.atan2(*x)));
                    DFFloat64Array::new_from_opt_iter(opt_iter)
                }
                _ => {
                    let y_series = y_column.to_minimal_array()?;
                    let x_series = x_column.to_minimal_array()?;
                    let opt_iter = y_series
                        .f64()?
                        .into_no_null_iter()
                        .zip(x_series.f64()?.into_no_null_iter())
                        .map(|(y, x)| Some(y.atan2(*x)));
                    DFFloat64Array::new_from_opt_iter(opt_iter)
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
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricCosFunction;

impl TrigonometricCosFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::COS)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricTanFunction;

impl TrigonometricTanFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::TAN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricCotFunction;

impl TrigonometricCotFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::COT)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricAsinFunction;

impl TrigonometricAsinFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ASIN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricAcosFunction;

impl TrigonometricAcosFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ACOS)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricAtanFunction;

impl TrigonometricAtanFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ATAN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

pub struct TrigonometricAtan2Function;

impl TrigonometricAtan2Function {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ATAN2)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}
