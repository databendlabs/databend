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
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

/// we can change this into the style of String2StringFunction in the future
pub trait NumberResultFunction<R> {
    const IS_DETERMINISTIC: bool;
    const MAYBE_MONOTONIC: bool;

    fn return_type() -> Result<DataType>;
    fn to_number(_value: &[u8]) -> R;
}

#[derive(Clone, Debug)]
pub struct String2NumberFunction<T, R> {
    display_name: String,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

impl<T, R> String2NumberFunction<T, R>
where
    T: NumberResultFunction<R> + Clone + Sync + Send + 'static,
    R: DFPrimitiveType + Clone,
    DFPrimitiveArray<R>: IntoSeries,
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(String2NumberFunction::<T, R> {
            display_name: display_name.to_string(),
            t: PhantomData,
            r: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default().num_arguments(1);

        if T::IS_DETERMINISTIC {
            features = features.deterministic();
        }

        if T::MAYBE_MONOTONIC {
            features = features.monotonicity();
        }

        FunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }
}

/// A common function template that transform string column into string column
/// Eg: trim, lower, upper, etc.
impl<T, R> Function for String2NumberFunction<T, R>
where
    T: NumberResultFunction<R> + Clone + Sync + Send,
    R: DFPrimitiveType + Clone,
    DFPrimitiveArray<R>: IntoSeries,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if !args[0].is_numeric() && !args[0].is_string() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }
        T::return_type()
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let column = columns[0]
            .column()
            .cast_with_type(&DataType::String)?
            .to_minimal_array()?
            .string()?
            .apply_cast_numeric(T::to_number);

        let column: DataColumn = column.into();
        Ok(column.resize_constant(input_rows))
    }
}

impl<T, R> fmt::Display for String2NumberFunction<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
