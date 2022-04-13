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
use std::time::Duration;

use common_datavalues::DataValue;
use common_datavalues::Int8Type;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::assert_numeric;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct SleepFunction {
    display_name: String,
}

impl SleepFunction {
    pub fn try_create(
        display_name: &str,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        assert_numeric(args[0])?;
        Ok(Box::new(SleepFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(1))
    }
}

impl Function for SleepFunction {
    fn name(&self) -> &str {
        "SleepFunction"
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        Ok(Int8Type::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<common_datavalues::ColumnRef> {
        let c = columns[0].column();
        if c.len() != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "The argument of function {} must be constant.",
                self.display_name
            )));
        }

        let value = c.get(0);
        let err = || ErrorCode::BadArguments(format!("Incorrect arguments to sleep: {}", value));

        // is_numeric is checked in return_type()
        // value >= 0  is checked here
        let duration = if c.data_type_id().is_floating() {
            value
                .as_f64()
                .and_then(|v| Duration::try_from_secs_f64(v).map_err(|_| err()))?
        } else {
            value.as_u64().map(Duration::from_secs).map_err(|_| err())?
        };

        if duration.ge(&Duration::from_secs(3)) {
            return Err(ErrorCode::BadArguments(format!(
                "The maximum sleep time is 3 seconds. Requested: {:?}",
                duration
            )));
        };
        std::thread::sleep(duration);
        let t = Int8Type::arc();
        t.create_constant_column(&DataValue::UInt64(0), input_rows)
    }
}

impl fmt::Display for SleepFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "sleep")
    }
}
