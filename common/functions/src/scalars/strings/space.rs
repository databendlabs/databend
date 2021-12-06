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

pub type SpaceFunction = SpaceGenFunction<SpaceGen>;

pub trait SpaceGenOperator: Send + Sync + Clone + Default + 'static {
    fn apply<'a>(&'a mut self, c: &u64) -> &'a [u8];
}

#[derive(Clone, Default)]
pub struct SpaceGen {
    buf: Vec<u8>,
}

impl SpaceGenOperator for SpaceGen {
    #[inline]
    fn apply<'a>(&'a mut self, c: &u64) -> &'a [u8] {
        self.buf.resize(*c as usize, 32);
        &self.buf[..]
    }
}

#[derive(Clone)]
pub struct SpaceGenFunction<T> {
    display_name: String,
    _marker: PhantomData<T>,
}

impl<T: SpaceGenOperator> SpaceGenFunction<T> {
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

impl<T: SpaceGenOperator> Function for SpaceGenFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args[0].is_unsigned_integer()
            && args[0] != DataType::String
            && args[0] != DataType::Null
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected unsigned integer or null, but got {}",
                args[0]
            )));
        }
        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let mut op = T::default();

        let r_column: DataColumn = match columns[0].column().cast_with_type(&DataType::UInt64)? {
            DataColumn::Constant(DataValue::UInt64(c), _) => {
                if let Some(c) = c {
                    DataColumn::Constant(
                        DataValue::String(Some(op.apply(&c).to_owned())),
                        input_rows,
                    )
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            DataColumn::Array(c_series) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for oc in c_series.u64()? {
                    r_array.append_option(oc.map(|c| op.apply(c)));
                }
                r_array.finish().into()
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl<F> fmt::Display for SpaceGenFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
