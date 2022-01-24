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
use crate::scalars::{Function2, Function2Description};
use crate::scalars::function_factory::FunctionFeatures;

#[derive(Clone, Debug)]
pub struct IfFunction2 {
    display_name: String,
}

impl IfFunction2 {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(IfFunction2 {
            display_name: display_name.to_string()
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().num_arguments(3);
        features = features.deterministic();
        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }
}

impl Function2 for IfFunction2 {
    fn name(&self) -> &str {
        "IfFunction"
    }

    fn get_monotonicity(&self, _args: &[crate::scalars::Monotonicity]) -> common_exception::Result<crate::scalars::Monotonicity> {
        unimplemented!()
    }

    fn return_type(&self, args: &[&common_datavalues2::DataTypePtr]) -> common_exception::Result<common_datavalues2::DataTypePtr> {
        unimplemented!()
    }

    fn eval(&self, _columns: &common_datavalues2::ColumnsWithField, _input_rows: usize) -> common_exception::Result<common_datavalues2::ColumnRef> {
        unimplemented!()
    }
}

impl std::fmt::Display for IfFunction2 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
