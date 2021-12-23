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
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

pub type LeftFunction = LeftRightFunction<Left>;
pub type RightFunction = LeftRightFunction<Right>;

pub trait LeftRightOperator: Send + Sync + Clone + Default + 'static {
    fn apply<'a>(&'a mut self, str: &'a [u8], i: &u64) -> &'a [u8];
}

#[derive(Clone, Default)]
pub struct Left {}

impl LeftRightOperator for Left {
    #[inline]
    fn apply<'a>(&'a mut self, str: &'a [u8], i: &u64) -> &'a [u8] {
        if *i == 0 {
            return &str[0..0];
        }
        let i = *i as usize;
        if i < str.len() {
            return &str[0..i];
        }
        str
    }
}

#[derive(Clone, Default)]
pub struct Right {}

impl LeftRightOperator for Right {
    #[inline]
    fn apply<'a>(&'a mut self, str: &'a [u8], i: &u64) -> &'a [u8] {
        if *i == 0 {
            return &str[0..0];
        }
        let i = *i as usize;
        if i < str.len() {
            return &str[str.len() - i..];
        }
        str
    }
}

#[derive(Clone)]
pub struct LeftRightFunction<T> {
    display_name: String,
    _marker: PhantomData<T>,
}

impl<T: LeftRightOperator> LeftRightFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            _marker: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl<T: LeftRightOperator> Function for LeftRightFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args[0].is_numeric() && args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[0]
            )));
        }
        if !args[1].is_unsigned_integer()
            && args[1] != DataType::String
            && args[1] != DataType::Null
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1]
            )));
        }
        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let mut op = T::default();

        let s_column = columns[0].column().cast_with_type(&DataType::String)?;
        let i_column = columns[1].column().cast_with_type(&DataType::UInt64)?;

        let r_column: DataColumn = match (s_column, i_column) {
            // #00
            (
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(i), _),
            ) => {
                if let (Some(s), Some(i)) = (s, i) {
                    DataColumn::Constant(
                        DataValue::String(Some(op.apply(&s, &i).to_owned())),
                        input_rows,
                    )
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #10
            (DataColumn::Array(s_series), DataColumn::Constant(DataValue::UInt64(i), _)) => {
                if let Some(i) = i {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for os in s_series.string()? {
                        r_array.append_option(os.map(|s| op.apply(s, &i)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #01
            (DataColumn::Constant(DataValue::String(s), _), DataColumn::Array(i_series)) => {
                if let Some(s) = s {
                    let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                    for oi in i_series.u64()? {
                        r_array.append_option(oi.map(|i| op.apply(&s, i)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #11
            (DataColumn::Array(s_series), DataColumn::Array(i_series)) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for s_i in izip!(s_series.string()?, i_series.u64()?) {
                    r_array.append_option(match s_i {
                        (Some(s), Some(i)) => Some(op.apply(s, i)),
                        _ => None,
                    });
                }
                r_array.finish().into()
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl<F> fmt::Display for LeftRightFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
