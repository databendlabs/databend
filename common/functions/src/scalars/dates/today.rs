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
use std::ops::Sub;

use common_datavalues::chrono::Date;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct TodayFunction {
    display_name: String,
}

impl TodayFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(TodayFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for TodayFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date16)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        let utc: Date<Utc> = Utc::now().date();
        let epoch = NaiveDate::from_ymd(1970, 1, 1);

        let duration = utc.naive_utc().sub(epoch);
        let value = DataValue::UInt16(Some(duration.num_days() as u16));
        Ok(DataColumn::Constant(value, input_rows))
    }
}

impl fmt::Display for TodayFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "today()")
    }
}
