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

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct SleepFunction {
    display_name: String,
}

impl SleepFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
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
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        if !args[0].data_type_id().is_numeric() {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal type {} of argument of function {}, expected numeric",
                args[0].data_type_id(),
                self.display_name
            )));
        }
        Ok(Int8Type::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let c = columns[0].column();
        if c.len() != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "The argument of function {} must be constant.",
                self.display_name
            )));
        }

        let seconds = c.get_u64(0)?;
        std::thread::sleep(Duration::from_secs(seconds));
        let t = Int8Type::arc();
        t.create_constant_column(&DataValue::UInt64(0), input_rows)
    }
}

impl fmt::Display for SleepFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "sleep")
    }
}
