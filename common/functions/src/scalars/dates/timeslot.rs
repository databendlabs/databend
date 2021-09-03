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

use common_datavalues::chrono::NaiveDateTime;
use common_datavalues::chrono::Timelike;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct TimeSlotFunction {
    display_name: String,
}

impl TimeSlotFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(TimeSlotFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for TimeSlotFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::DateTime32)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let fmt = "%Y-%m-%d %H:%M:%S";
        let time_str: DataValue = columns[0].column().clone().to_values()?[0].clone();
        let parse_result = NaiveDateTime::parse_from_str(time_str.to_string().as_str(), fmt);
        let date = parse_result.unwrap();
        let minute = date.minute();
        let mut new_minute: u32 = 0;
        if minute >= 30 {
            new_minute = 30;
        }
        let result = date.with_minute(new_minute);
        let new_date = result.unwrap();
        let value = DataValue::UInt32(Some(new_date.timestamp() as u32));
        Ok(DataColumn::Constant(value, input_rows))
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for TimeSlotFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "timeSlot")
    }
}
