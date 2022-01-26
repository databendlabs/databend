// Copyright 2022 Datafuse Labs.
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
use common_exception::Result;

use super::logic2::LogicFunction2;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
pub struct LogicXorFunction2 {
    _display_name: String,
}

impl LogicXorFunction2 {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().num_arguments(2);
        features = features.deterministic();
        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }
}

impl Function2 for LogicXorFunction2 {
    fn name(&self) -> &str {
        "LogicXorFunction"
    }

    fn return_type(
        &self,
        args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        unimplemented!()
    }

    fn eval(
        &self,
        _columns: &common_datavalues2::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        unimplemented!()
    }
}

impl std::fmt::Display for LogicXorFunction2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}
