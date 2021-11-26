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

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

pub trait TrimOperator: Send + Sync + Clone + 'static {
    fn trim(_: &[u8]) -> &[u8];
}

#[derive(Clone)]
pub struct LTrim;

impl TrimOperator for LTrim {
    fn trim(s: &[u8]) -> &[u8] {
        for (idx, ch) in s.iter().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                return &s[idx..];
            }
        }
        b""
    }
}

#[derive(Clone)]
pub struct RTrim;

impl TrimOperator for RTrim {
    fn trim(s: &[u8]) -> &[u8] {
        for (idx, ch) in s.iter().rev().enumerate() {
            if *ch != b' ' && *ch != b'\t' {
                return &s[..s.len() - idx];
            }
        }
        b""
    }
}

#[derive(Clone)]
pub struct TrimFunction<T> {
    _display_name: String,
    _mark: PhantomData<T>,
}

impl<T: TrimOperator> TrimFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            _display_name: display_name.to_string(),
            _mark: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl<T: TrimOperator> Function for TrimFunction<T> {
    fn name(&self) -> &str {
        &self._display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0] != DataType::String && args[0] != DataType::Null {
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

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let column: DataColumn = columns[0]
            .column()
            .cast_with_type(&DataType::String)?
            .to_minimal_array()?
            .string()?
            .into_iter()
            .fold(
                StringArrayBuilder::with_capacity(columns[0].column().len()),
                |mut builder, s| {
                    builder.append_option(s.map(T::trim));
                    builder
                },
            )
            .finish()
            .into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl<F> fmt::Display for TrimFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self._display_name)
    }
}

pub type LTrimFunction = TrimFunction<LTrim>;
pub type RTrimFunction = TrimFunction<RTrim>;
