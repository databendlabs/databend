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

use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct TimeFunction<T> {
    display_name: String,
    t: PhantomData<T>,
}

pub trait NoArgTimeFunction {
    fn execute() -> u32;
}

#[derive(Clone)]
pub struct TimeSlot;

impl NoArgTimeFunction for TimeSlot {
    fn execute() -> u32 {
        let utc = Utc::now();
        (utc.timestamp_millis() / 1000 - 60 * 30) as u32
    }
}

impl<T> TimeFunction<T>
where T: NoArgTimeFunction + Clone + Sync + Send + 'static
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(TimeFunction::<T> {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }
}

impl<T> Function for TimeFunction<T>
where T: NoArgTimeFunction + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::DateTime32)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let value = T::execute();
        Ok(DataColumn::Constant(
            DataValue::UInt32(Some(value)),
            input_rows,
        ))
    }
}

impl<T> fmt::Display for TimeFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub type TimeSlotFunction = TimeFunction<TimeSlot>;
