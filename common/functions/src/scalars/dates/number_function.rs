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

use common_datavalues::arrays::DFUInt32Array;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct NumberFunction<T> {
    display_name: String,
    t: PhantomData<T>,
}

pub trait NumberResultFunction {
    fn execute(_value: DateTime<Utc>) -> u32;
}

#[derive(Clone)]
pub struct ToYYYYMM;

impl NumberResultFunction for ToYYYYMM {
    fn execute(_value: DateTime<Utc>) -> u32 {
        _value.year() as u32 * 100 + _value.month()
    }
}

impl<T> NumberFunction<T>
where T: NumberResultFunction + Clone + Sync + Send + 'static
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(NumberFunction::<T> {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }
}

impl<T> Function for NumberFunction<T>
where T: NumberResultFunction + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        println!(
            "_columns len:{:?}, dataColumn:{:?}, field:{:?}",
            _columns.len(),
            _columns[0].column(),
            _columns[0].field()
        );

        if _columns.len() == 0 {
            return Result::Err(ErrorCode::UnknownFunction(
                "Number of arguments for function toYYYYMM doesn't match: passed 0, should be 1 or 2")
            );
        }

        let data_type = _columns[0].data_type();
        let number_vec = match data_type {
            DataType::Date16 | DataType::Date32 => {
                let seconds_array: Vec<Option<u32>> = _columns[0]
                    .column()
                    .to_values()?
                    .iter()
                    .map(|value| match value {
                        DataValue::UInt16(Some(v)) => {
                            let date_time = Utc.timestamp(*v as i64 * 24 * 3600, 0_u32);
                            Some(T::execute(date_time))
                        }
                        DataValue::UInt32(Some(v)) => {
                            let date_time = Utc.timestamp(*v as i64 * 24 * 3600, 0_u32);
                            Some(T::execute(date_time))
                        }
                        _ => None,
                    })
                    .collect();
                Ok(seconds_array)
            },
            DataType::DateTime32 => {
                let seconds_array: Vec<Option<u32>> = _columns[0].column().to_values()?
                    .iter()
                    .map(|value| {
                        match value {
                        DataValue::UInt32(Some(v)) => {
                            let date_time = Utc.timestamp(*v as i64, 0_u32);
                            Some(T::execute(date_time))
                        },
                        _ => None,
                    }
                    }).collect();

                Ok(seconds_array)    
            },
            other => Result::Err(ErrorCode::IllegalDataType(format!(
               "Illegal type {:?} of argument of function toYYYYMM.Should be a date16/data32 or a dateTime32",
                other))),
        }?;
        Ok(DFUInt32Array::from_iter(number_vec).into())
    }
}

impl<T> fmt::Display for NumberFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub type ToYYYYFunction = NumberFunction<ToYYYYMM>;
