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
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct AngleFunction<T> {
    _display_name: String,
    t: PhantomData<T>,
}

pub trait AngleConvertFunction {
    fn convert(v: f64) -> f64;
}

impl<T> AngleFunction<T>
where T: AngleConvertFunction + Clone + Sync + Send + 'static
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(AngleFunction::<T> {
            _display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl<T> Function for AngleFunction<T>
where T: AngleConvertFunction + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        "AngleFunction"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::Float64)?
            .f64()?
            .apply_cast_numeric(T::convert);
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl<T> fmt::Display for AngleFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self._display_name)
    }
}

#[derive(Clone)]
pub struct ToDegrees;

impl AngleConvertFunction for ToDegrees {
    fn convert(v: f64) -> f64 {
        v.to_degrees()
    }
}

#[derive(Clone)]
pub struct ToRadians;

impl AngleConvertFunction for ToRadians {
    fn convert(v: f64) -> f64 {
        v.to_radians()
    }
}

pub type DegressFunction = AngleFunction<ToDegrees>;
pub type RadiansFunction = AngleFunction<ToRadians>;
