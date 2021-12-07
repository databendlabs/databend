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
use common_exception::ErrorCode;
use common_exception::Result;

// use common_tracing::tracing;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

pub trait StringOperator: Send + Sync + Clone + Default + 'static {
    fn apply<'a>(&'a mut self, _: &'a [u8], _: &mut [u8]) -> usize;
    fn estimate_bytes(&self, array: &DFStringArray) -> usize {
        array.inner().values().len()
    }
}

/// A common function template that transform string column into string column
/// Eg: trim, lower, upper, etc.
#[derive(Clone)]
pub struct String2StringFunction<T> {
    display_name: String,
    _marker: PhantomData<T>,
}

impl<T: StringOperator> String2StringFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            _marker: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl<T: StringOperator> Function for String2StringFunction<T> {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args[0].is_numeric() && args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }

        Ok(DataType::String)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let mut op = T::default();

        let array = columns[0]
            .column()
            .cast_with_type(&DataType::String)?
            .to_minimal_array()?;

        let estimate_bytes = op.estimate_bytes(array.string()?);
        // tracing::error!("bytes_total: {}", estimate_bytes);

        let column: DataColumn = array
            .string()?
            // TODO(veeupup) use into_no_null_iter to make branch prediction works good
            .into_iter()
            .fold(
                EfficientStringArrayBuilder::with_capacity(
                    columns[0].column().len(),
                    estimate_bytes,
                ),
                |mut builder, s| {
                    match s {
                        Some(x) => {
                            builder.write_values(|buffer| op.apply(x, buffer));
                        }
                        None => {
                            builder.write_null();
                        }
                    }
                    builder
                },
            )
            .finish()
            .into();
        Ok(column.resize_constant(input_rows))
    }
}

impl<F> fmt::Display for String2StringFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
