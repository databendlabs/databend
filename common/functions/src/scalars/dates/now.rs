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
use std::sync::Arc;

use common_datavalues2::chrono::DateTime;
use common_datavalues2::chrono::Utc;
use common_datavalues2::prelude::*;
use common_exception::Result;

use crate::scalars::function2_factory::Function2Description;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;

// TODO: try move it to simple function?
#[derive(Clone)]
pub struct NowFunction {
    display_name: String,
}

impl NowFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(NowFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default())
    }
}
impl Function2 for NowFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        Ok(DateTime32Type::arc(None))
    }

    fn eval(
        &self,
        _columns: &common_datavalues2::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        let utc: DateTime<Utc> = Utc::now();
        let value = (utc.timestamp_millis() / 1000) as u32;
        let column = Series::from_data(&[value as u32]);
        Ok(Arc::new(ConstColumn::new(column, input_rows)))
    }
}

impl fmt::Display for NowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "now()")
    }
}
