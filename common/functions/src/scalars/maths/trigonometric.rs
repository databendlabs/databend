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
}

impl fmt::Display for Trigonometric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Trigonometric::SIN => "sin",
            Trigonometric::COS => "cos",
            Trigonometric::TAN => "tan",
            Trigonometric::COT => "cot",
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
        1
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::Float64)?
            .f64()?
            .apply_cast_numeric(|v| match self.t {
                Trigonometric::COS => v.cos(),
                Trigonometric::SIN => v.sin(),
                Trigonometric::COT => 1.0 / v.tan(),
                Trigonometric::TAN => v.tan(),
            });
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
